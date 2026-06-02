# FastAPI 调度服务

这是文档中成员 B 的服务骨架，负责 AMR 任务接收、调度分配、车辆心跳和状态回调，可以单独运行、单独部署。

当前 MVP 已包含：

- `POST /task/submit`：接收上游 WMS/MES 任务。
- `POST /dispatch/run`：模拟调度 Worker，给空闲 AMR 分配任务。
- `POST /vehicles/heartbeat`：接收成员 A Adapter 转发的车辆心跳。
- `POST /rmf/callback`：接收 RMF/Adapter 状态回调。
- `GET /tasks`、`GET /vehicles`、`GET /events`：查询接口。

后续替换方向：

- 内存/SQL 查询替换为 Redis ZSET 调度池。
- `scheduler.py` 中的贪心分配替换为 OR-Tools VRP。
- `rmf/callback` 与真实 OpenRMF Bridge 对接。

## MySQL 配置

服务优先读取项目根目录的 `.env`，也支持直接读取以下环境变量：

```powershell
$env:WMS_MYSQL_HOST="127.0.0.1"
$env:WMS_MYSQL_PORT="3306"
$env:WMS_MYSQL_DATABASE="wms_platform"
$env:WMS_MYSQL_USER="root"
$env:WMS_MYSQL_PASSWORD="123456"
```

服务不会自动创建表。建表参考：

```text
..\docs\mysql_schema_reference.sql
```
