# 本地湖仓风格数据摄取服务（local-lakehouse-ingest）

一个不依赖外部数据库的**本地湖仓**：用文件系统承载表数据与元数据，提供
**批次幂等摄取、结构校验与演进、基于版本快照的读取与时间旅行**。
在重复上报、结构变更与并发读写场景下，结果保持一致、可追溯，且失败原因可区分。

- 语言/运行时：Python ≥ 3.14，FastAPI
- 存储：纯本地文件（原子写 `os.replace` + `fsync`、`fcntl.flock` 跨进程锁、JSONL 快照、JSON catalog）
- 无状态服务进程：状态全部落盘，进程重启后可完整恢复

---

## 1. 目录布局

```
<data_root>/
  orders.lock                 # 表级提交互斥锁（flock）
  orders/
    catalog.json              # 元数据：提交点历史、批次索引、过期版本（权威事实来源）
    data/
      v-000000000001.jsonl    # 每个版本一个不可变全量快照
      v-000000000002.jsonl
      ...
```

- `catalog.json` 是唯一的“真相来源”：记录每个提交点（版本号、批次 ID、结构、
  快照文件、记录数、提交时间、父版本、批次负载指纹）以及 `batch_id -> version` 索引。
- 每个版本的数据文件**一旦发布即不可变**，里面是该提交点的**全量快照**
  （历史行按新结构前滚 + 本批新增行）。

---

## 2. 提交语义（commit）

### 2.1 批次与幂等（exactly-once 效果）

- 每次提交必须携带调用方生成的非空 **`batch_id`**。
- 首次提交成功后，版本号、批次负载指纹（按提交结构投影后做 SHA-256）固化进 catalog。
- **重复提交 / 重试**：相同 `batch_id` + 相同结构 + 相同负载，直接返回**首次提交的版本结果**，
  数据**不重复、不新增版本**。
- 相同 `batch_id` 但**结构或负载不同**：返回 `409 batch_conflict`（绝不静默覆盖）。
- 批次指纹存于 catalog 而非数据文件，因此**即使该版本快照已过期删除，幂等判定依然有效**。

### 2.2 原子性（整体生效 / 整体不生效）

提交在表级排他锁内串行执行，顺序为：

1. 加锁，载入 catalog；
2. （可选）乐观并发检查 `base_version`；
3. 幂等检查；
4. 结构兼容校验；
5. 逐记录校验与投影（任一条非法 → 整批拒绝，不写任何文件）；
6. 基于父快照做结构前滚，生成全量行；
7. 写**新的**不可变快照文件（临时文件 + `fsync` + `os.replace` 原子落盘）；
8. 把新提交点追加进 catalog，并对 `catalog.json` 做**一次原子替换发布**。

读者只会看到“catalog 未更新（旧版本）”或“catalog 已更新（新版本完整可见）”两个状态，
不会读到半个版本。失败发生在 catalog 发布之前时，只可能残留一个未被引用的快照文件
（不会被读到，也不影响后续版本号）。

### 2.3 乐观并发控制

提交时可带 `base_version`（客户端读取时基于的版本）。若其与当前版本不一致，
返回 `409 concurrent_modification`，避免过期客户端覆盖他人提交。

---

## 3. 结构兼容与演进规则（schema evolution）

字段为有序列表；每个字段含 `name`、`type`、`nullable`（默认 true）、可选 `default`。

支持类型：`long`（64 位整数）、`double`、`string`、`boolean`、`timestamp`（微秒整数）。

| 变更 | 是否允许 | 说明 / 失败 `sub_reason` |
| --- | --- | --- |
| 尾部**新增** `nullable` 字段 | ✅ | 历史行该列稳定取 `null` |
| 尾部新增带显式 `default` 的字段（含 NOT NULL） | ✅ | 历史行稳定回填默认值 |
| `long` → `double` | ✅ | 唯一允许的**类型拓宽**；历史整数在新快照中规范化为 `double`，老快照原值不变 |
| `required` → `nullable` | ✅ | 放宽约束 |
| **删除字段** | ❌ | `field_removed`（并给出字段名） |
| 在中间插入 / 重排字段 | ❌ | `field_removed`（新字段只能尾部追加） |
| 类型**收窄**（如 `double→long`） | ❌ | `type_narrowed` |
| 其它不兼容类型变更（如 `long→string`） | ❌ | `type_changed` |
| `nullable` → `NOT NULL` | ❌ | `nullable_narrowed` |
| 新增 `NOT NULL` 且无默认值字段 | ❌ | `new_column_missing_default` |
| 首次提交无结构 / 空结构 | ❌ | `empty_schema` |

记录级校验（同样整体拒绝，附字段名）：

- 出现结构中不存在的字段：`record_unknown_field`；
- NOT NULL 字段缺值：`record_missing_field`；
- 值与字段类型不符 / 向 NOT NULL 写 null：`record_type_mismatch`。

> 设计原则：**新增字段缺省语义确定且对历史数据稳定生效；任何删除、收窄或不兼容变更
> 都被明确拒绝，绝不静默丢弃或改写既有数据。** 历史快照文件保持其当时的结构与取值。

---

## 4. 快照读取与时间旅行

- `read()` / `GET /current`：读取当前最新已发布版本。
- `read(version=n)` / `GET /snapshots/n`：读取指定版本的不可变快照；
  后续继续写入后，该结果仍可**逐字节复现**。
- `read(as_of=ISO8601)`：时间旅行，返回提交时间 `<= as_of` 的最新版本
  （无时区的时间串按 UTC 解释）。
- 并发写入期间读者**无锁**，但每次读取都基于一个已发布提交点，
  快照行数恒等于 catalog 中该版本的 `record_count`，不会撕裂。

版本错误明确区分：

| 情形 | 异常 `reason` | HTTP |
| --- | --- | --- |
| 版本号从未存在 / 超出当前版本 / `as_of` 早于首次提交 | `version_not_found` | 404 |
| 版本曾存在但快照已过期被清理 | `version_expired` | 410 |

### 快照保留（过期）

`expire_versions(retain=N)` 只保留最近 N 个快照，删除更旧快照的数据文件并在 catalog 中
标记为 `expired`。**catalog 的提交历史与批次索引永久保留**，因此：

- 历史仍可通过 `/history` 追溯；
- 过期版本读取得到明确的 `410 version_expired`；
- 旧批次的幂等重放仍可凭 catalog 中的指纹判定。

---

## 5. HTTP 接口

服务入口：`main:app`（数据根目录默认 `./.lakehouse-data`，可用
`lakehouse.api.create_app(data_root=...)` 自定义）。

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET  | `/health` | 健康检查 |
| POST | `/tables/{table}/commits` | 提交批次（见下） |
| GET  | `/tables/{table}/current` | 当前版本快照 |
| GET  | `/tables/{table}/snapshots/{version}` | 指定版本快照 / 时间旅行 |
| GET  | `/tables/{table}/history` | 提交历史（含过期标记） |
| POST | `/tables/{table}/expire?retain=N` | 过期旧快照 |

提交请求体：

```json
{
  "batch_id": "batch-0001",
  "records": [{"id": 1, "tag": "a"}],
  "schema": {
    "columns": [
      {"name": "id", "type": "long", "nullable": false},
      {"name": "tag", "type": "string"}
    ]
  },
  "base_version": 3
}
```

- 后续提交若结构不变，可省略 `schema`（沿用当前结构）。
- 成功返回 `{table, version, batch_id, record_count, parent_version, created_at, decision}`。
- 错误体统一为 `{error, message, table, ...}`，结构类错误额外带
  `sub_reason` 与 `field`，便于程序化区分失败原因。

直接以库方式使用：

```python
from lakehouse.table import LakehouseTable

t = LakehouseTable("./.lakehouse-data", "orders")
t.commit("b1", [{"id": 1}], {"columns": [{"name": "id", "type": "long", "nullable": False}]})
print(t.read(1))          # 时间旅行
print(t.read())           # 当前快照
print(t.history())        # 提交历史
```

---

## 6. 本地验证方法

```bash
# 1) 安装依赖（含 dev 组的 pytest / httpx）
uv sync

# 2) 运行全部测试（61 个用例：重复批次 / 不兼容结构 / 多线程与多进程并发读写）
uv run pytest -q

# 3) 启动服务
uv run uvicorn main:app --reload

# 4) 手工验证
curl -s localhost:8000/health
curl -s -X POST localhost:8000/tables/orders/commits \
  -H 'content-type: application/json' \
  -d '{"batch_id":"b1","records":[{"id":1}],"schema":{"columns":[{"name":"id","type":"long","nullable":false}]}}'
curl -s localhost:8000/tables/orders/current
curl -s localhost:8000/tables/orders/snapshots/1
# 重复提交同一批次 -> 返回同一 version，数据不重复
curl -s -X POST localhost:8000/tables/orders/commits \
  -H 'content-type: application/json' \
  -d '{"batch_id":"b1","records":[{"id":1}],"schema":{"columns":[{"name":"id","type":"long","nullable":false}]}}'
```

测试日志（`lakehouse` logger，INFO 级别）会打印**提交版本、批次标识与判定依据**，例如：

```
INFO commit accepted table=orders version=1 batch_id=b1 added=1 total=1 columns=1 decision=accept
INFO commit replay  table=orders version=1 batch_id=b1 ... decision=idempotent_replay
WARNING commit rejected ... batch_id=b1 existing_version=1 decision=reject reason=batch_conflict
INFO snapshot read  table=orders version=1 batch_id=b1 rows=1 decision=snapshot_hit
```

---

## 7. 测试覆盖一览

- `tests/test_types.py`：五类逻辑类型的值校验；拓宽 / 收窄 / 类型变更 / 删字段 /
  非尾部新增 / 缺省值 / nullable 收窄等兼容规则与可区分子原因。
- `tests/test_table.py`：重复批次幂等、冲突可区分、失败整批回滚（all-or-nothing）、
  快照不可变且可复现、时间旅行、版本不存在/已过期区分、过期后历史与幂等仍有效、重启恢复。
- `tests/test_schema_evolution.py`：新增 nullable/带默认值字段的历史回填、
  long→double 前滚、删除/收窄/空结构拒绝且数据不变。
- `tests/test_concurrency.py`：8 线程 × 25 批并发提交无空洞无重复；
  同批次并发重试只生效一次；并发冲突负载只接受其一；读者恒见一致提交点；
  `base_version` 乐观冲突；以及 4 进程跨进程（flock）串行化。
- `tests/test_api.py`：HTTP 成功路径、幂等、409/422/404/410 错误码与 `sub_reason`、
  history 与 expire 接口。
