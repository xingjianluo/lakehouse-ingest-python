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
  持锁后先从磁盘重新读取提交日志再判定。同一台机器上任意多个后台任务、
  终端进程对同一张表的提交被串行化，每次成功提交都落在**独立的新版本**上，
  不会互相覆盖或吞掉。
- **原子提交点**：提交点 = 元数据日志追加成功。版本数据文件先于日志落盘
  （临时文件 + `os.replace` 原子改名），读取方在共享锁内确定提交点后再读
  数据文件，因此写入进行中只能看到某个**完整的提交点**，永远读不到写了一半
  的批次。
- **崩溃安全**：写入方在"数据已落盘、日志未追加"之间崩溃 = 未提交。
  重试时版本号从日志重算，孤儿数据文件被整体重写，数据不多也不少；
  进程退出时 flock 由内核自动释放，不会留下死锁。写入方在**追加日志途中**
  崩溃会留下半截日志行：读取时残缺行被忽略（视为未提交），后续每次追加
  之前会先把这种半截残留截断，保证每条新日志都是完整一行——只要提交接口
  成功返回了版本号，重新打开表就一定能读到这一版，后续提交不会被残留
  "吞掉"。多进程同时写、其中某个进程中途挂掉后重试，同样满足该保证。

## 批次重试与对账

每个批次按"批次号 + 内容指纹（**批次记录内容**的规范哈希，记录键序无关）"
判定：

| 情况 | 结果 |
| --- | --- |
| 新批次号 | 校验通过后提交为新版本，返回版本号 |
| 批次号已提交、记录内容一致 | 幂等重试：直接返回首次提交的版本号，**不再写任何数据** |
| 批次号已提交、记录内容变了 | 抛 `BatchConflictError`（携带 `batch_id` 与已提交的 `committed_version`），不产生任何写入 |

- 重试时**是否重复携带表结构、结构字段的书写顺序如何，都不影响判定**：
  只要记录内容一致就按重试处理；只有记录内容真的变了才判定冲突。
- 多个写入端同时重试同一批次：最多一个真正写进去，内容一致的其余写入端
  拿到同一个版本号，内容不一致的拿到明确的冲突失败。
- **对账查询**：`table.batch_status(batch_id)` 返回
  `committed`（落在哪个版本、**该批次自身多少条记录**，不是全表累计）/
  `rejected`（可程序识别的 `reason` 与判定依据 `detail`）/ `unknown`。
  被结构或记录校验拒绝的批次会追加到 `rejections.jsonl`，同样查得到。
- 被拒绝的批次**不留数据、不占版本**，且拒绝记录不参与后续提交判定：
  同一批次号修正内容后重试，按全新提交正常受理。

## 结构兼容规则与预检

允许范围收敛到"已存数据的类型/取值与声明的结构永远对得上"的变更：

| 变更 | 结果 |
| --- | --- |
| 新增字段（类型已知，缺省值与声明类型匹配，`default` 可为 `None`） | 允许；缺省值对历史数据稳定生效 |
| 既有字段类型变更（含 `int -> float` 加宽） | 拒绝：历史行读出来与新声明类型对不上 |
| 删除字段 | 拒绝，`reason=field_removed` |
| 类型加宽（`int -> float`） | 拒绝，`reason=type_widened`（历史整数行与声明的 float 对不上） |
| 类型收窄（`float -> int`） | 拒绝，`reason=type_narrowed` |
| 跨类型族变更（`int/float -> string`、`string -> bool` 等） | 拒绝，`reason=type_incompatible`（历史值类型与新声明对不上） |
| 新字段缺省值与声明类型不匹配 | 拒绝，`reason=default_type_mismatch` |
| 未知类型名 | 拒绝，`reason=unknown_type` |

所有拒绝都抛出 `IncompatibleSchemaError`，其 `reason` 字段
（`SchemaRejectionReason` 枚举）可区分具体失败原因；拒绝发生在任何写入
之前，不留数据、不占版本，同一批次号之后重试不受本次失败影响。

为保证"落盘数据类型与声明结构一致"，`float` 字段的整数值与整型缺省值
在写入时被无损规范化为浮点（整数是浮点子集），读回来始终是浮点。

**预检**：`table.check_schema(schema)` 只检查不写入，返回
`SchemaCheckResult(accepted, merged_schema, reason, field, detail)`。
预检与提交共用同一份判定逻辑，结论必然一致（预检拒绝的原因就是提交
抛出的原因），可安全用于上线前检查与文档生成。

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

- `tests/test_crash_recovery.py`：半截提交日志残留不影响后续提交（返回的
  版本重开必读得到、后续提交不被吞）、进程崩溃后多进程继续提交全部可见、
  孤儿数据文件被整体重写、半截拒绝日志不影响对账；
- `tests/test_duplicate_batch.py`：同号同内容重试返回首次结果（含不重复
  携带表结构、结构字段乱序两种重试形态）、同号不同内容冲突失败（可定位
  批次与已落版本）、崩溃后重试不多不少、进程重启后重试幂等、批次对账
  （条数为本批自身条数）、被拒绝批次重试按全新提交判定；
- `tests/test_concurrent_processes.py`：多进程并发写同一张表（版本唯一）、
  多进程同号同内容重试（只有一个真写）、多进程同号不同内容（恰好一个落地）；
- `tests/test_concurrent_rw.py`：多线程并发读写快照一致性、并发写入版本号
  唯一、多任务同号重试、历史快照可复现；
- `tests/test_schema_precheck.py`：预检结论与真实提交一致（放行/各类拒绝，
  含 int->float 加宽拒绝）、被拒绝的变更无残留、预检本身无副作用；
- `tests/test_schema_evolution.py`：新增字段缺省语义、float 字段整数值
  规范化为浮点、删除/加宽/收窄/不兼容变更的可区分拒绝；
- `tests/test_time_travel.py`：逐版本时间旅行、不存在/已过期版本的明确拒绝；
- `tests/test_api.py`：HTTP 层提交、幂等重试、批次冲突 409、批次对账、
  结构预检、快照错误码。

## HTTP 接口

```bash
uv run fastapi dev main.py        # 启动服务（表目录由 LAKEHOUSE_TABLE_ROOT 指定，默认 ./data/orders）
```

- `POST /batches`：提交批次，Body 为 `{"batch_id", "records", "schema?"}`。
  成功返回 `201 + {"version", "batch_id", "deduplicated"}`；同号同内容的
  幂等重试返回相同版本号且 `deduplicated=true`；同号不同内容返回
  `409 batch_conflict`（含已提交版本号）；结构不兼容返回
  `422 incompatible_schema`（含可区分的 `reason` 与 `field`）；记录校验失败
  返回 `422 record_validation`。
- `GET /batches/{batch_id}`：批次对账。已提交返回
  `{"status": "committed", "version", "record_count"}`（`record_count`
  为该批次自身的条数）；被拒绝返回
  `{"status": "rejected", "reason", "detail"}`；未知批次返回
  `404 batch_not_found`。
- `POST /schema/check`：结构预检，Body 为 `{"schema": [...]}`，返回
  `{"accepted", "reason"?, "field"?, "detail"?}`，不写任何数据。
- `GET /snapshot?version=N`：读取快照；版本不存在返回 `404 version_not_found`，
  已过期返回 `410 version_expired`（含最早可用版本）。
- `GET /version`：当前最新提交版本号。
