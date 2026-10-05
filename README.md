# lakehouse-ingest-python

本地湖仓风格的数据摄取服务：以本地文件存储承载表数据与元数据，
保证多写入端并发、批次重试、结构变更场景下结果一致且可对账。

## 存储布局

每张表对应一个本地目录：

```
<table-root>/
├── metadata/log.jsonl         # 提交日志：每行一个版本条目（版本号、批次标识、结构、行数、内容指纹）
├── metadata/rejections.jsonl  # 拒绝日志：被结构/记录校验拒绝的批次（对账用）
├── metadata/.lock             # 表级 flock 锁文件（跨进程互斥）
└── data/v<version>.jsonl      # 每个版本的全量快照数据
```

## 并发保证（多任务 / 多进程 / 多终端）

- **串行化提交**：提交临界区 = 进程内线程锁 + 表级 `fcntl.flock` 排他锁，
  持锁后先修复崩溃残留，再从磁盘重新读取提交日志后判定。同一台机器上
  任意多个后台任务、终端进程对同一张表的提交被串行化，每次成功提交都
  落在**独立的新版本**上，不会互相覆盖或吞掉。
- **原子提交点**：提交点 = 元数据日志追加成功（含 `fsync`）。版本数据
  文件先于日志落盘（临时文件 + `os.replace` 原子改名 + 目录 fsync），
  读取方在共享锁内确定提交点后再读数据文件，因此写入进行中只能看到某个
  **完整的提交点**，永远读不到写了一半的批次。
- **崩溃安全（半截提交残留不影响后续提交）**：写入端可能死在两种状态——
  数据文件已写但日志未追加（孤儿数据文件），或日志只追加了半截行
  （没有换行、无法解析）。下一次提交拿到排他锁后会：
  1. 把提交日志、拒绝日志末尾的残缺行**截断**到最后一个完整行；
  2. 清理数据目录里的孤儿临时文件。

  由于截断发生在排他锁内，残留内容只可能来自已经死亡的进程，不会误伤
  任何进行中的提交。因此**只要接口成功返回了版本号，重新打开表就一定
  读得到这一版，数据不多不少；之后每次提交也都正常生效**，不会出现
  "新版本被半截行遮蔽、后续提交持续被吞"。孤儿版本数据文件会在重试时
  被整体重写。多个进程同时写、其中某个进程中途挂掉后再重试，满足同样
  的保证（flock 由内核在进程退出时自动释放，不留死锁）。

## 批次重试与对账

每个批次按"批次号 + 内容指纹"判定。内容指纹由**记录内容**与**本次
声明的结构意图**两部分组成：记录按 JSON 规范化后哈希（记录在 list 中
的顺序参与、单条记录内字段的书写顺序不参与）；结构按字段语义规范化
（字段书写顺序不参与），"不携带结构"本身也是一种意图。

| 情况 | 结果 |
| --- | --- |
| 新批次号 | 校验通过后提交为新版本，返回版本号 |
| 批次号已提交、记录内容一致（无论重试时是否重复带结构、字段书写顺序是否相同） | 幂等重试：直接返回首次提交的版本号，**不再写任何数据、不报冲突** |
| 批次号已提交、记录内容变了 | 抛 `BatchConflictError`（携带 `batch_id` 与已提交的 `committed_version`），不产生任何写入 |
| 记录相同但声明了与首次不同的结构 | 按冲突失败（声明意图不一致） |

- 典型重试姿势都被接受：首次 `commit_batch(id, records, schema)`，重试
  `commit_batch(id, records)`（不带结构）或把 schema 字段顺序换一换，
  只要数据内容一样，一律返回首次版本号。
- 多个写入端同时重试同一批次：最多一个真正写进去，内容一致的其余写入端
  拿到同一个版本号，内容不一致的拿到明确的冲突失败。
- **对账查询**：`table.batch_status(batch_id)` 返回
  `committed`（落在哪个版本 `version`、**该批次自己带来的条数**
  `record_count`，不是整张表当前总条数）/ `rejected`（可程序识别的
  `reason`、判定依据 `detail`；冲突失败时还带 `committed_version`，
  能看出是哪个批次、已经落在哪个版本）/ `unknown`。成功与失败都查得到。
  已落地批次后来又发生过失败尝试（例如冲突重试）时，`committed` 结果里
  还会带 `last_rejection_reason` / `last_rejection_detail`。
- 被结构或记录校验拒绝的批次会追加到 `rejections.jsonl`，同样查得到；
  **不留数据、不占版本**，且拒绝记录不参与后续提交判定：同一批次号
  修正内容后重试，按全新提交正常受理。

## 结构兼容规则与预检

收敛目标：**任何被放行的变更，都必须让已存数据读出来的类型与声明的
结构对得上**。历史行按写入时的原样存放，不做重写，因此已存字段的类型
一律不允许变更。

| 变更 | 结果 |
| --- | --- |
| 新增字段（类型已知，缺省值与声明类型匹配，`default` 可为 `None`） | 允许；缺省值对历史数据稳定生效 |
| 已存字段改类型：`int -> float` | 拒绝，`reason=type_mismatch`（历史行读出来仍是 int，与声明的 float 对不上） |
| 已存字段改类型：`float -> int` | 拒绝，`reason=type_narrowed`（收窄丢精度） |
| 已存字段跨类型族（`int/float -> string`、`string -> bool` 等） | 拒绝，`reason=type_incompatible` |
| 删除字段 | 拒绝，`reason=field_removed` |
| 新字段缺省值与声明类型不匹配 | 拒绝，`reason=default_type_mismatch` |
| 未知类型名 | 拒绝，`reason=unknown_type` |

所有拒绝都抛出 `IncompatibleSchemaError`，其 `reason` 字段
（`SchemaRejectionReason` 枚举）可区分具体失败原因、`field` 指出涉及
字段；拒绝发生在任何写入之前，不留数据、不占版本。

**预检**：`table.check_schema(schema)` 只检查不写入，返回
`SchemaCheckResult(accepted, merged_schema, reason, field, detail)`。
预检与提交共用同一份判定逻辑（`_evaluate`），结论必然一致——预检说
不能接受，真实提交一定以同一个原因被拒绝；预检说接受，提交一定成功。
预检本身纯只读，可安全用于上线前检查与文档生成。

## 快照与时间旅行

- `snapshot(version)` 读取指定版本的全量快照；`snapshot()` 读最新版本。
- 历史快照以全量数据文件承载，后续继续写入后仍可复现。
- 请求从未存在的版本抛 `VersionNotFoundError`；请求已被
  `expire_versions(keep_last)` 清理的版本抛 `VersionExpiredError`
  （携带最早可用版本号）。版本清理只删数据文件，元数据日志完整保留。

## 本地验证

```bash
uv sync                 # 安装依赖（含 dev 组 pytest）
uv run pytest -q        # 运行全部测试
uv run pytest -s        # 查看测试日志：批次编号、提交版本与判定依据
```

测试覆盖：

- `tests/test_crash_recovery.py`：半截日志行不遮蔽后续提交、重开表读到
  完整新版本、后续提交不再被吞、孤儿临时文件清理、拒绝日志残缺恢复、
  多进程某进程中途死亡后存活进程提交与重交全部持久；
- `tests/test_duplicate_batch.py`：同号同内容重试返回首次结果、不带结构
  重试 / 字段书写顺序不同按重试处理、不同结构意图仍算冲突、同号不同内容
  冲突失败（含已落版本）、对账条数=本批条数、冲突尝试在对账中可见、
  崩溃后重试不多不少、进程重启后重试幂等、被拒绝批次重试按全新提交判定；
- `tests/test_concurrent_processes.py`：多进程并发写同一张表（版本唯一）、
  多进程不带结构同号重试（只有一个真写）、多进程同号同内容重试、
  多进程同号不同内容（恰好一个落地）；
- `tests/test_concurrent_rw.py`：多线程并发读写快照一致性、并发写入版本号
  唯一、多任务同号重试、历史快照可复现；
- `tests/test_schema_precheck.py`：预检结论与真实提交一致（放行/各类
  拒绝）、已存字段任何类型变更（int->float / float->int / 跨类型族）
  原因可区分且拒绝无残留、被拒绝变更后同号重试正常、预检无副作用；
- `tests/test_schema_evolution.py`：新增字段缺省语义、删除/收窄/
  历史数据类型对不上等变更的可区分拒绝；
- `tests/test_time_travel.py`：逐版本时间旅行、不存在/已过期版本的明确拒绝；
- `tests/test_api.py`：HTTP 层提交、幂等重试、批次冲突 409、批次对账、
  结构预检、快照错误码。

## HTTP 接口

```bash
uv run fastapi dev main.py        # 启动服务（表目录由 LAKEHOUSE_TABLE_ROOT 指定，默认 ./data/orders）
```

- `POST /batches`：提交批次，Body 为 `{"batch_id", "records", "schema?"}`。
  成功返回 `201 + {"version", "batch_id", "deduplicated"}`；同号同内容的
  幂等重试（含不带 schema、schema 字段顺序不同）返回相同版本号且
  `deduplicated=true`；同号不同内容返回 `409 batch_conflict`（含已提交
  版本号 `committed_version`）；结构不兼容返回
  `422 incompatible_schema`（含可区分的 `reason` 与 `field`）；记录校验
  失败返回 `422 record_validation`。
- `GET /batches/{batch_id}`：批次对账。已提交返回
  `{"status": "committed", "version", "record_count",
  "last_rejection_reason"?, "last_rejection_detail"?}`，其中
  `record_count` 是该批次自己的条数；被拒绝返回
  `{"status": "rejected", "reason", "detail", "committed_version"?}`；
  未知批次返回 `404 batch_not_found`。
- `POST /schema/check`：结构预检，Body 为 `{"schema": [...]}`，返回
  `{"accepted", "reason"?, "field"?, "detail"?}`，不写任何数据。
- `GET /snapshot?version=N`：读取快照；版本不存在返回 `404 version_not_found`，
  已过期返回 `410 version_expired`（含最早可用版本）。
- `GET /version`：当前最新提交版本号。
