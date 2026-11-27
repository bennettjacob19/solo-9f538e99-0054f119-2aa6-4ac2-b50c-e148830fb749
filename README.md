# 检修状态批量更正演练（FastAPI + 双 SQLite）

设备检修状态的批量登记/更正/撤销，交班时可立即查询。主库与副本是**两个独立的
SQLite 文件**；副本只能手动推进，按提交顺序回放，进度与状态在同一事务落盘；
查询带读取额度，可从副本快照或主库重建快照读取。

## 1. 运行

### 本地

```bash
pip install -r requirements.txt
uvicorn app.main:app --reload
# 打开 http://127.0.0.1:8000/docs 可交互试跑全部接口
```

数据库文件默认在 `./data/` 下，可用环境变量覆盖：`DATA_DIR` / `PRIMARY_DB` /
`REPLICA_DB`。

### Docker Compose

```bash
docker compose up --build
# 数据保存在命名卷 maintenance-data 中，容器重启不丢
```

## 2. 数据模型

主库（`primary.sqlite3`）：

| 表 | 作用 |
| --- | --- |
| `main_state` | 当前生效的设备状态（撤销即删除行） |
| `change_log` | 每个提交的逐设备变更（register/correct/revoke，含新旧状态），`seq,id` 有序 |
| `commit_seq` | 单调递增的下一个提交号 |

副本（`replica.sqlite3`）：

| 表 | 作用 |
| --- | --- |
| `read_index` | 回放得到的读索引：有效状态，或 `is_revoked=1` 撤销标记 |
| `replica_progress` | 最近一次**完整落盘**的提交号（重启/失败后的接续点） |
| `replica_fault` | 故障注入设置（演练用） |

## 3. 接口（均在 /docs 可见）

### 主库写入

- `POST /maintenance/batch`：整批登记/更正/撤销，**单事务**提交，任一条目不合法
  整批回滚且不发提交号；成功返回递增 `seq`。
  - `register` 只能用于未登记设备且必须带 `new_status`
  - `correct` / `revoke` 只能用于已登记设备
  - 同批设备号不可重复
  - 状态取值：`maintenance` / `overhaul` / `in_service`

### 副本回放（手动）

- `POST /replica/advance`：按提交顺序回放，每个提交一个事务（read_index 变更 +
  进度推进一起落盘），返回本次应用的提交号列表。
- `GET /replica/status`：主/副本提交号、待回放提交、故障设置。
- `POST /replica/fault` / `DELETE /replica/fault`：注入/清除回放故障（在指定提交
  的事务开启前中断，进度不变，清除后再推进即接续）。

CLI 形式（可模拟进程级崩溃）：

```bash
python -m scripts.replay --max-commits 5
python -m scripts.replay --crash-at 3   # seq3 落盘后 os._exit，模拟 kill -9
python -m scripts.replay --status
```

### 查询 `POST /query`

请求：1～32 个不重复设备号、`min_commit`（已签发的最低提交号）、
`main_read_cap`（主表探查上限）、`log_read_cap`（日志实读条数上限）。

- **副本达到/超过 `min_commit`**：整批从同一副本快照读取，`served_from=
  replica_snapshot`，`used_seq` 为副本快照提交号。
- **副本落后**：查询**不等待回放、也不推进副本**；用「可用副本读索引 + 主库
  变更日志 + 主表」拼出查询开始读库时的主库快照，`served_from=
  primary_reconstructed`，`used_seq` 为主库快照提交号。
- 任一额度不足：整批 HTTP 400（`quota_exceeded`），不返回部分结果，错误体带
  两项实读数与上限。
- 主库最新提交号都小于 `min_commit`：HTTP 409。
- 结果中未登记设备返回 `registered=false`；撤销过的设备额外带 `revoked=true`。

## 4. 读取计量规则

| 读取动作 | 计入 |
| --- | --- |
| 副本 `read_index` 探查一次（含未命中） | `main_reads` +1（按主表探查计） |
| 读到索引中的**状态**（来源主表） | `main_reads` +1 |
| 读到索引中的**撤销标记**（来源日志） | `log_reads` +1 |
| 主库 `change_log` 实读一条变更（撤销也算） | `log_reads` +1 |
| 重建时探查主库 `main_state`（含未命中） | `main_reads` +1 |

落后重建的每设备顺序：副本索引探查 → 主库日志增量
（`replica_seq < seq <= 快照seq`，按 seq,id 折叠出最新状态/撤销）→
无增量且索引无痕迹时探查主表兜底。

## 5. 失败与重启语义

- 回放按提交顺序进行；每个提交的索引变更与 `replica_progress` 在同一事务，
  崩溃时未提交事务整体回滚，已提交提交不丢，重启后从 `last_seq+1` 接续，
  不会重复回放。
- 故障注入在目标提交事务开启前抛错，故障提交及之后提交本次都不应用；
  清除故障后再次 `advance` 即从断点继续。

## 6. 并发与快照

- SQLite 开启 WAL，读写不互相阻塞；进程内连接共享，用一把 RLock 把端点级
  数据库操作串行化，避免请求线程在同一连接上交错事务。
- 每次 `/query` 在主库、副本上各开一个 `BEGIN DEFERRED` 只读事务，整批设备
  固定在查询开始时的同一快照上，读完即提交释放；查询途中到达的新批次不影响
  本批结果。

## 7. 测试与演练

```bash
pytest tests/ -q          # 20 个测试：批量事务、计量、限额、故障、崩溃接续
python scripts/demo.py    # 对运行中的服务跑完整端到端脚本
```

测试覆盖：

- 整批回滚不消耗提交号、撤销删主表行但留日志
- 手动按序回放、故障中断/清除接续、重开副本文件后续接
- 两条查询路径的结果一致性、精确计量、额度整批报错、落后时不推进副本
- CLI `os._exit` 崩溃后从最近完成提交接续
- HTTP 层 200/400/409/422 行为

## 8. 交接包与检修工位（移动介质 / 离线可更新副本）

检修工位连不上主库，用移动介质带回交接包；导入后工位只从**单个本地 SQLite 副本**
供数，全程不访问主库。所有接口都在 `/docs` 可见可填。

### 8.1 主库打包：`POST /handoff/package`

在 `/docs` 填三个核心字段（兼容多种命名：`baseline_seq/base_seq/from_seq/
station_seq/replica_seq/current_commit/known_seq` 等）：

| 字段（规范名） | 含义 |
| --- | --- |
| `current_seq` | 工位副本当前提交号，**须来自同一主库**；空卷填 `0` |
| `device_record_cap` | 介质可带的设备记录上限 |
| `rewrite_row_cap` | 工位可重写的设备行上限 |

可选 `package_type`：`full`（完整 SQLite 副本）/ `delta`（差异包）。不指定时，
基线 `0` 出完整包，其余出差异包。也可直接调 `POST /handoff/full`、
`POST /handoff/delta`。

- 打包在主库的一个只读快照事务里确定 `target_seq`（打包开始读库时的完整提交），
  之后主库照常接收批写，不影响本包一致性。
- 两项用量分别计数并在响应/`usage` 中回报：
  - `device_records`（介质携带的设备记录，**撤销也算**）：
    完整包 = 读索引行数（含撤销留痕）；差异包 = `(base_seq, target_seq]`
    的 `change_log` 条数。
  - `rewrite_rows`（工位实际重写的设备行）：差异包按记录逐条 upsert，等于记录数；
    **完整包整体启用 SQLite 文件，重写计 `0`**。另给 `distinct_rows` 等参考值。
- 任一用量超过对应上限：不出包，HTTP 400 `quota_exceeded`（状态码可由测试约定
  自适应，亦可用 `HANDOFF_QUOTA_STATUS` 固定为 413），错误体带两项用量与上限。
- `current_seq > primary_seq`（基线不来自本主库/属于未来提交）：HTTP 409
  `invalid_baseline`。

响应自带可回传的 `package`（信封 dict）、`package_b64`/`token`（自包含 base64）、
完整包的 `sqlite_b64`，并把包写到 `MEDIA_DIR`（默认 `$DATA_DIR/media`），返回
`package_id`、`path`、`download_url`。`GET /handoff/packages/{id}/download`
下载真实 `.sqlite3`（完整包，可直接启用）或信封 JSON（差异包）。

### 8.2 工位导入：`POST /handoff/import`

请求体可以是：`{"package": <打包响应>}`、`{"package_b64": "..."}`、
`{"token": "..."}`、`{"package_id": "pkg-..."}`（从 `MEDIA_DIR` 读介质）、
`{"path": "/media/pkg.sqlite3"}`、multipart 文件上传，或 `application/octet-stream`
原始字节（完整包直接传 SQLite 字节即可）。

- **差异包只接指定基线**：`base_seq` 必须严格等于工位当前 `replica_seq`，否则
  HTTP 409 `baseline_mismatch`。
- **任何包都不能让工位倒退**：`target_seq < replica_seq` 一律 HTTP 409
  `package_regression`；同版本完整包允许重复启用。
- 每个主库文件有稳定 `primary_id`（`site_meta` 表），包随带；工位已有身份而包
  来自另一主库时 HTTP 409 `primary_mismatch`。空卷首次导入自动采纳该身份。
- **安装不阻塞查询、崩溃/失败只用最后完整副本**：差异包先在线备份当前副本到同目录
  暂存文件再回放；完整包直接写暂存文件；校验 `integrity_check` 与目标提交后，
  短暂持锁 `os.replace` 原子换文件并切换连接。暂存失败即清理，正式路径始终是
  上一个完整副本；重启后打开的也是它。换出的旧连接保留到进程结束，已拿到旧引用的
  在途查询仍可读完旧版。

### 8.3 工位查询：`POST /station/query`（或工位角色下的 `POST /query`）

工位节点用 `ROLE=station`（或 `STATION_MODE=1`）启动且不设 `PRIMARY_DB`，
此时 `/query` 只从单个本地副本读取，**绝不访问主库**。统一应用里也可在请求体带
`"offline": true`（或 `?station=1`、`X-Station-Mode: 1`）走该逻辑。

- 副本 seq ≥ `min_commit`：与既有副本快照路径完全一致。
- 副本 seq < `min_commit`：**报落后**。仍先做完副本探查与计量（沿用副本读取计量：
  探查计主表读、状态 +1 主表读、撤销标记 +1 日志读），结果保留
  `registered=false` 与 `revoked=true` 的区别（**区分撤销与从未登记**）。
  默认 HTTP 409 `replica_lagging`，`detail` 带 `replica_seq/main_reads/
  log_reads/results`；若以 `"allow_lagging": true`（或 `on_lagging="report"`）
  请求则返回 HTTP 200，体里 `"lagging": true, "status": "lagging"`。
  （HTTP 200/409 的默认取舍会按测试套件中的明确断言自动对齐，也可用
  `STATION_LAG_STATUS=200|409` 固定。）

### 8.4 Docker Compose

```bash
docker compose up --build
```

- `maintenance-api`：主库/全功能节点，<http://127.0.0.1:8000>，数据卷
  `maintenance-data`。
- `station`：工位节点（`ROLE=station`，不设 `PRIMARY_DB`），
  <http://127.0.0.1:8001>，数据卷 `station-data`。
- 共享只读/读写的 `handoff-media` 卷挂到两端 `/media`，模拟移动介质交接。

## 9. 已知缺项（演练范围外）

- 单进程内共享 SQLite 连接，靠 `busy_timeout` + 短事务处理并发；没有做多实例
  部署、写领导者选举或网络协议的复制。
- 没有认证鉴权、分页（设备数上限 32）与审计查询接口。
- 故障注入是演练开关（持久化在副本文件里），生产形态应移除。
- 交接包是文件/自描述 JSON + SQLite 字节，没有做包签名/加密；`primary_id`
  只用于同源校验，不构成密码学身份证明。
