# 演示脚本（约 5 分钟）

## 前提条件

- Docker 全部容器运行中（`docker compose ps` 确认）
- 后端运行中（`http://localhost:8080/api/health` → `ok`）
- 前端运行中（`http://localhost:3000`）
- 日志模拟器运行中（`log-simulator/simulator.py`）

---

## 演示流程

### 1. 整体架构介绍（30 秒）

```
用户请求 → Nginx → Filebeat → Kafka → Flink → ClickHouse → Spring Boot API → Vue 3 大屏
                                                                         ↕ (WebSocket)
```

数据流路径：
- **采集层**：Nginx 日志 → Filebeat → Kafka
- **计算层**：Flink 流处理（1 分钟 Tumbling Window）
- **存储层**：ClickHouse（列存，秒级聚合查询）
- **展示层**：Spring Boot REST + WebSocket → Vue 3 + ECharts

### 2. 实时大屏展示（2 分钟）

**打开**：`http://localhost:3000`

**核心指标卡（上半部分）**：
- **实时 PV**：当前累积 PV（如 `37,733`），每次 WebSocket 推送更新
- **今日 UV**：独立访客数（如 `6,016`）
- **平均响应时间**：最近 5 分钟均值（如 `245ms`）
- **5xx 错误**：错误数及闪烁告警效果

**PV 实时趋势图**：折线图，每 5 秒更新一次，保留最近 60 个时间点

**热门页面 TOP10**：柱状图（渐变色），/index 领先

**流量来源**：环形饼图（直接访问、搜索引擎、外部链接）

**状态码分布**：环形图（2xx 绿色、4xx 橙色、5xx 红色）

**转化漏斗**：Flink 漏斗分析作业产出后展示

### 3. 分析页面介绍（1 分 30 秒）

| 页面 | 路由 | 功能 |
|------|------|------|
| PV 趋势分析 | `/analysis/pv` | 15/30/60/120 分钟时间范围切换 |
| UV 分析 | `/analysis/uv` | UV 大数字展示 + 趋势线 |
| 来源分析 | `/analysis/referer` | 饼图 + 明细表格 |
| 性能监控 | `/analysis/performance` | 状态码柱状图 + KPI |
| 漏斗分析 | `/analysis/funnel` | 漏斗图 + 转化率表 |

### 4. 弹性演示（1 分钟）

**停止模拟器**：
```bash
docker compose exec nginx sh -c "pkill -f simulator.py"
```
→ 观察：数据停止更新，图表静止

**重启模拟器**：
```bash
docker compose exec nginx sh -c "python3 /simulator.py &"
```
→ 观察：1 分钟内图表恢复更新，数据连续性良好

### 5. 性能验证（可选）

**验证 ClickHouse 查询速度**：
```bash
docker compose exec clickhouse clickhouse-client \
  --query "SELECT sum(pv_count) FROM pv_minute WHERE window_start >= now() - INTERVAL 1 HOUR"
```

**验证 Redis 缓存**：
```bash
docker compose exec redis redis-cli KEYS 'analytics:*'
```
预期看到：pv, uv, topPages:10, statusCodes, avgRt

---

## 当前系统状态

| 组件 | 状态 | 路径 |
|------|------|------|
| Nginx | ✅ HTTP 200 | `http://localhost:80` |
| Kafka | ✅ 消息持续消费 | `localhost:9092` |
| Flink | ✅ Job `nginx-log-analysis` RUNNING | Checkpoint: 每 10s / 3ms |
| ClickHouse | ✅ pv_minute 表持续写入 | `localhost:8123` |
| Redis | ✅ 缓存命中 | `localhost:6379` |
| Backend API | ✅ /api/overview → 16ms | `http://localhost:8080` |
| Frontend | ✅ Vite 开发服务器 | `http://localhost:3000` |
| 模拟器 | ✅ ~3 QPS 持续写入 | Nginx access.log |

## 压力测试参考数据（100 QPS下）

| 指标 | 数值 |
|------|------|
| ClickHouse 写入延迟 | < 5 秒（Flink 窗口闭合后） |
| API 响应时间 | 15-18ms |
| Kafka Lag | 稳定（无积压增长） |
| Flink Checkpoint | 3-4ms，每 10s |
| 系统 CPU | ClickHouse ~14%, 其余 < 2% |
