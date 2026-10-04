# lakehouse-ingest-python

本地湖仓风格的数据摄取服务：以本地文件存储承载表数据与元数据，
在**多后台任务、多终端进程同时写入**的环境下，保证每次提交独立完整、
批次可重试可对账、结构变更规则明确且可提前预检，所有结果一致且可追溯。

## 存储布局

每张表对应一个本地目录：

```
<table-root>/
├── metadata/
│   ├── lock             # 跨进程互斥锁（fcntl.flock 劝告锁）
│   ├── log.jsonl        # 提交日志：每行一个版本条目（版本号、批次标识、结构、
│   │                    #   全量行数、本批行数、归一化内容指纹）
│   └── rejected.jsonl   # 被拒批次台账（冲突 / 结构 / 记录校验），不占版本
└── data/v<version>.jsonl   # 每个版本的全量快照数据（每行一条记录）
```

## 多写入端的并发保证

- **双层串行化**：同一进程内的多个线程由进程内 `RLock` 串行化；
  不同进程（不同终端、不同后台任务）由 `metadata/lock` 上的
  `flock` 排他锁串行化。每次提交都在锁内**从磁盘重读日志**后再判定，
  因此多个写入端看到的状态始终最新。
- **每次成功提交都是独立的一份**：版本号在锁内分配并顺序追加，
  并发的不同批次各自获得互不相同的版本，不会互相覆盖或吞掉。
- **读取只看到完整提交点**：版本数据文件先写临时文件、再 `os.replace`
  原子改名，最后才追加日志（日志追加成功 = 提交点）。读快照时在
  共享锁内确定版本条目，再读不可变的版本数据文件，写入进行中读到的
  永远是某个完整版本，不会读到写了一半的批次。
- **同一批次并发重试，最多一份**：多个写入端同时提交相同的
  `batch_id` + 相同内容时，只有第一个抢到锁的真正落盘，其余拿到
  `DuplicateBatchError`（携带首次版本号与批次条数），调用方按成功
  重试处理即可，不重复写任何数据。
- **写入端中途挂掉再重试，不多不少**：
  - 崩溃发生在提交点（日志追加）之前：数据走临时文件不会留下半截
    版本；日志若有崩溃残留的残缺尾行，会在下个提交持锁时被截断
    （视为未提交），重试正常补提；
  - 崩溃发生在提交点之后（响应丢失）：重试按指纹判定为幂等重试，
    指向同一版本，不重复、不丢失。

## 批次重试与内容冲突的对账语义

每次成功提交都在日志中保存该批次归一化内容的 SHA-256 **指纹**
（目标结构字段定义 + 归一化后的记录）。

| 情形 | 判定 | 行为 |
| --- | --- | --- |
| 新 `batch_id` | 首次提交 | 正常落新版本 |
| 同 `batch_id`，指纹一致 | 幂等重试 | 抛 `DuplicateBatchError`，带 `committed_version`、`record_count`，**不写任何数据**，直接把首次结果还给调用方 |
| 同 `batch_id`，指纹不一致 | 内容冲突 | 抛 `BatchConflictError`，带 `committed_version`（指出它已落在哪个版本）；明确的程序化失败，**不写任何数据** |

- 冲突、结构拒绝、记录校验失败都会追加一条记录到
  `metadata/rejected.jsonl` **被拒台账**，方便排查；
  台账只用于查询，**不参与提交判定**，也不占用版本号。
- 用 `Table.batch_status(batch_id)` 按批次号查处理情况：
  - `state="committed"`：`version`（落在哪个版本）、`batch_size`（多少条）；
    若此前有过被拒（如先冲突后查证），`rejections` 里带历次原因；
  - `state="rejected"`：从未成功落过版本，`rejections` 带每次被拒的
    `reason` / `detail` / `schema_reason` / `field` / `committed_version`；
  - `state="unknown"`：日志与台账都没有。

## 结构兼容规则（明确、可预判、可写进文档）

唯一原则：**只有不改变已存数据语义的变更才允许。**

| 变更 | 结果 |
| --- | --- |
| 追加新字段，`default` 与声明类型一致（`default=None` 表示可空） | 允许；缺省值对历史快照稳定生效 |
| 删除既有字段 | 拒绝，`reason=field_removed` |
| 既有字段改类型——**任何**改动，包括 `int -> float` 这类加宽 | 拒绝，`reason=type_changed` |
| 新字段缺省值与声明类型不匹配 | 拒绝，`reason=default_type_mismatch` |

为什么加宽也不允许：声明类型一旦从 `int` 变成 `float`，历史行里按
`int` 落盘的原值语义就与新声明对不上（之前遇到的“历史行还是原类型”
即源于此）。允许范围因此收敛为「只追加带合法缺省值的新字段」。

所有拒绝在**真正写入之前**抛出 `IncompatibleSchemaError`，
`reason`（`SchemaRejectionReason` 枚举）与 `field` 可区分具体原因；
拒绝的提交不写数据文件、不追加提交日志、不占版本号；同批次随后
改回兼容方案再提交，判定不受这次失败影响（失败只记台账）。

### 只检查不提交：结构预检

```python
result = table.precheck_schema(new_schema)
if result.compatible:
    merged = result.merged_schema      # 合并后的结构
else:
    result.reason, result.field, result.detail  # 不能接受的具体原因
```

预检执行的就是提交时的同一个 `check_compatibility`，**不写任何数据、
不记台账**，因此「预检可接受 ⇔ 提交时结构判定通过」恒成立。
HTTP 层对应 `POST /schema/precheck`。

## 快照与时间旅行

- `snapshot(version)` 读指定版本的全量快照；`snapshot()` 读最新版本。
- 历史快照以全量数据文件承载，提交后即不可变，后续继续写入仍可复现。
- 请求从未存在的版本抛 `VersionNotFoundError`；请求已被
  `expire_versions(keep_last)` 清理的版本抛 `VersionExpiredError`
  （携带最早可用版本号）。版本清理只删数据文件，元数据日志完整保留。

## 本地验证

```bash
uv sync                 # 安装依赖（含 dev 组 pytest）
uv run pytest -q        # 运行全部测试
uv run pytest -s        # 查看测试日志：批次编号、提交版本与判定依据
```

测试覆盖（日志中均带批次号、版本号、判定依据）：

- `tests/test_multiprocess_writers.py`：**多个进程**同时写同一张表
  （版本独立、无覆盖无吞批次）；多个进程同时重试同一批次（恰好一份、
  其余幂等返回同一版本）；子进程提交后被杀、重试不多不少；
- `tests/test_batch_reconciliation.py`：同批次号内容一致（幂等、不写数据、
  带回首次结果）与不一致（冲突、指出已落版本、无写入）；
  被拒批次进台账、可按批次号查询原因；
- `tests/test_schema_precheck.py`：预检只检查不写数据；预检结论与真正
  提交结论一致（同因同字段）；被拒结构变更无数据/版本/临时文件残留，
  同批次随后用兼容结构仍可提交；
- `tests/test_schema_evolution.py`：新增字段缺省语义、可空字段，
  删字段 / 改类型（含加宽）/ 缺省值不匹配的可区分拒绝；
- `tests/test_concurrent_rw.py`：多线程并发读写下快照一致性、
  并发写入版本号唯一、历史快照可复现；
- `tests/test_duplicate_batch.py`：重试 / 重启后重试的幂等性；
- `tests/test_time_travel.py`：逐版本时间旅行、不存在 / 已过期版本的明确拒绝；
- `tests/test_api.py`：HTTP 层全部判定码。

## HTTP 接口

```bash
uv run fastapi dev main.py        # 表目录由 LAKEHOUSE_TABLE_ROOT 指定，默认 ./data/orders
```

- `POST /batches`：Body `{"batch_id", "records", "schema?"}`：
  - 成功 `201 {"version", "batch_id"}`；
  - 一致重试 `409 duplicate_batch`（含 `committed_version`、`record_count`、
    `idempotent=true`，调用方按成功处理）；
  - 内容冲突 `409 batch_conflict`（含已落版本 `committed_version`）；
  - 结构不兼容 `422 incompatible_schema`（含可区分的 `reason`、`field`）；
  - 记录校验失败 `422 record_validation`。
- `GET /batches/{batch_id}`：批次对账；`state` 为 `committed` /
  `rejected`，带版本、条数与历次 `rejections`；未知批次 `404 batch_unknown`。
- `POST /schema/precheck`：Body 同提交（`schema` 必填），返回
  `{"compatible", "reason", "field", "detail"}`，不写任何数据。
- `GET /snapshot?version=N`：读快照；版本不存在 `404 version_not_found`，
  已过期 `410 version_expired`（含最早可用版本）。
- `GET /version`：当前最新提交版本号。
