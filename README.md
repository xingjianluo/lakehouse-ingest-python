# lakehouse-ingest-python

本地湖仓风格的数据摄取服务：以本地文件存储承载表数据与元数据，
保证重复上报、结构变更与并发读写场景下结果一致且可追溯。

## 存储布局

每张表对应一个本地目录：

```
<table-root>/
├── metadata/log.jsonl      # 提交日志：每行一个版本条目（版本号、批次标识、结构、行数）
└── data/v<version>.jsonl   # 每个版本的全量快照数据
```

## 提交语义

- **原子生效**：提交点 = 元数据日志追加成功。版本数据文件先于日志落盘
  （临时文件 + `os.replace` 原子改名），因此一个批次要么整体生效、
  要么整体不生效；崩溃残留的残缺日志行会被忽略，视为未提交。
- **批次幂等**：`batch_id` 全局唯一。重复提交或重试同一批次时抛出
  `DuplicateBatchError`（携带已提交的版本号），数据与元数据保持不变，
  不会重复也不会丢失。
- **记录校验**：记录中的未知字段、值类型不匹配都会被
  `RecordValidationError` 拒绝，不静默丢弃或改写；缺失字段按字段定义的
  `default` 补齐。

## 结构兼容规则

提交时可携带新结构，相对当前结构校验演进兼容性：

| 变更 | 结果 |
| --- | --- |
| 新增字段（带确定缺省值，`default` 可为 `None`） | 允许；缺省值对历史数据稳定生效 |
| 类型加宽（`int -> float -> string`） | 允许 |
| 删除字段 | 拒绝，`reason=field_removed` |
| 类型收窄（如 `float -> int`） | 拒绝，`reason=type_narrowed` |
| 跨类型族变更（如 `string -> bool`） | 拒绝，`reason=type_incompatible` |

所有拒绝都抛出 `IncompatibleSchemaError`，其 `reason` 字段
（`SchemaRejectionReason` 枚举）可区分具体失败原因，且拒绝前不产生任何写入。

## 快照与时间旅行

- `snapshot(version)` 读取指定版本的全量快照；`snapshot()` 读最新版本。
- 读取先在锁内确定提交点（版本条目），再读数据文件：并发写入期间
  看到的始终是某个一致提交点，不会读到半个批次。
- 历史快照以全量数据文件承载，后续继续写入后仍可复现。
- 请求从未存在的版本抛 `VersionNotFoundError`；请求已被
  `expire_versions(keep_last)` 清理的版本抛 `VersionExpiredError`
  （携带最早可用版本号）。版本清理只删数据文件，元数据日志完整保留。

## 本地验证

```bash
uv sync                 # 安装依赖（含 dev 组 pytest）
uv run pytest -q        # 运行全部测试
uv run pytest -s        # 查看测试日志：提交版本、批次标识与判定依据
```

测试覆盖：

- `tests/test_duplicate_batch.py`：重复批次 / 重试 / 重启后重试的幂等性；
- `tests/test_schema_evolution.py`：新增字段缺省语义、类型加宽放行，
  删除字段 / 类型收窄 / 不兼容变更的可区分拒绝；
- `tests/test_concurrent_rw.py`：多线程并发读写下快照一致性、
  并发写入版本号唯一、历史快照可复现；
- `tests/test_time_travel.py`：逐版本时间旅行、不存在 / 已过期版本的明确拒绝。

## HTTP 接口

```bash
uv run fastapi dev main.py        # 启动服务（表目录由 LAKEHOUSE_TABLE_ROOT 指定，默认 ./data/orders）
```

- `POST /batches`：提交批次，Body 为 `{"batch_id", "records", "schema?"}`；
  成功返回 `201 + {"version", "batch_id"}`；重复批次返回 `409 duplicate_batch`
  （含已提交版本号）；结构不兼容返回 `422 incompatible_schema`（含可区分的
  `reason` 与 `field`）；记录校验失败返回 `422 record_validation`。
- `GET /snapshot?version=N`：读取快照；版本不存在返回 `404 version_not_found`，
  已过期返回 `410 version_expired`（含最早可用版本）。
- `GET /version`：当前最新提交版本号。
