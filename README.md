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

## 8. 已知缺项（演练范围外）

- 单进程内共享 SQLite 连接，靠 `busy_timeout` + 短事务处理并发；没有做多实例
  部署、写领导者选举或网络协议的复制。
- 没有认证鉴权、分页（设备数上限 32）与审计查询接口。
- 故障注入是演练开关（持久化在副本文件里），生产形态应移除。
- 未接入真实持久化设备/消息队列，副本数据源是同机主库文件而非跨节点传输。
