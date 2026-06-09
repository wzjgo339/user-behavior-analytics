# 用户行为实时分析平台

基于 Nginx → Filebeat → Kafka → Flink → ClickHouse → Spring Boot → Vue 3 的全链路实时数据分析平台。从 Nginx 访问日志中实时采集、计算 PV/UV/状态码/来源/漏斗等指标，通过 WebSocket 推送到暗色大屏展示。

---

## 架构

```
┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────────────┐
│  Nginx   │───▶│ Filebeat │───▶│  Kafka   │───▶│  Flink 流计算    │
│ 访问日志  │    │ 日志采集  │    │ 消息队列  │    │  1min 窗口聚合   │
└──────────┘    └──────────┘    └──────────┘    └────────┬─────────┘
                                                         │
                                                         ▼
┌──────────┐    ┌──────────────────┐    ┌──────────────────┐
│  Vue 3   │◀───│  Spring Boot 3   │◀───│   ClickHouse     │
│ 实时大屏   │    │  REST + WebSocket │    │   分析数据库      │
│ ECharts  │    │  + Redis 缓存     │    │   列式存储       │
└──────────┘    └──────────────────┘    └──────────────────┘
     ↕ WebSocket 实时推送 (5s 间隔)
```

---

## 技术栈

| 层级 | 技术 | 说明 |
|------|------|------|
| 🌐 反向代理 | **Nginx** (Docker) | 自定义日志格式 `analysis`，产生访问日志 |
| 📡 日志采集 | **Filebeat 8.x** (Docker) | 采集 access.log 写入 Kafka |
| 📨 消息队列 | **Kafka 3.x** (Docker) | 3 分区，topic: `nginx-access-log` |
| ⚡ 流计算 | **Flink 1.19** (Docker) | Tumbling Window 1min 聚合 PV/UV/来源/状态码 |
| 🗄️ 分析库 | **ClickHouse** (Docker) | 物化视图 + 跳数索引优化，查询 `< 100ms` |
| 🧠 缓存 | **Redis 7** (Docker) | 后端 API 缓存，TTL 10-30s |
| 🗃️ 业务库 | **MySQL 8** (Docker) | 用户配置等持久化存储 |
| 🔧 后端 | **Spring Boot 3 + JDK 17** | REST API + STOMP/SockJS WebSocket |
| 🎨 前端 | **Vue 3 + Vite 8 + ECharts 6** | 暗色大屏，响应式布局 |

---

## 快速启动

### 前置要求

- Docker Desktop（WSL2，建议 ≥ 4GB 内存）
- JDK 17+
- Maven 3.9+
- Node.js 18+

### 1. 启动基础设施

```bash
docker compose up -d
```

启动 8 个容器：Nginx、Kafka、Filebeat、Flink (jobmanager + taskmanager)、ClickHouse、Redis、MySQL。

### 2. 启动日志模拟器

```bash
docker compose exec -d nginx python3 /simulator.py
```

以 ~3 QPS 模拟用户访问，产生 Nginx 访问日志。

### 3. 启动后端

```bash
cd server
mvn clean package -DskipTests
java -jar target/analytics-server-1.0.0.jar
```

API 运行在 `http://localhost:8080`。

### 4. 启动前端

```bash
cd ui
npm install    # 首次运行需要
npm run dev
```

大屏访问 `http://localhost:3000`。

---

## 页面预览

| 页面 | 路由 | 内容 |
|------|------|------|
| 📊 实时大屏 | `/dashboard` | KPI 卡片（PV/UV/响应时间/5xx）+ 5 个 ECharts 图表 |
| 📈 PV 分析 | `/analysis/pv` | PV 趋势折线图，15/30/60/120 分钟切换 |
| 👥 UV 分析 | `/analysis/uv` | UV 大数字 + 趋势线 |
| 🔗 来源分析 | `/analysis/referer` | 来源饼图 + 明细表格 |
| ⏱ 性能监控 | `/analysis/performance` | 响应时间 + 状态码分布 |
| 🔄 漏斗分析 | `/analysis/funnel` | 用户转化漏斗图 |

---

## 数据流说明

```
用户请求网页
    │
    ▼
Nginx 记录 access.log（自定义 log_format）
    │
    ▼
Filebeat 实时 tail 日志文件 → Kafka (`nginx-access-log` topic)
    │
    ▼
Flink 消费 Kafka 消息 → 解析日志 → 1min Tumbling Window 聚合
    │  ├── PV 聚合 → pv_minute 表 (url, pv_count, avg_response_time)
    │  ├── UV 聚合 → uv_minute 表 (uv_count)
    │  ├── 状态码聚合 → status_minute 表 (status_code, count)
    │  └── 来源聚合 → referer_hourly 表 (referer_type, count)
    ▼
ClickHouse 列式存储，秒级聚合查询
    │
    ▼
Spring Boot 后端提供 REST API + WebSocket 推送
    │  ├── GET /api/overview — 概览数据
    │  ├── GET /api/pv/trend?minutes=60 — PV 趋势
    │  ├── GET /api/pv/top?limit=10 — 热门页面
    │  ├── GET /api/funnel — 漏斗分析
    │  └── WS /topic/realtime — 每 5s 推送实时数据
    ▼
Vue 3 大屏展示 + 实时更新
```

---

## 压力测试

100 QPS 压力测试下系统表现：

| 指标 | 数值 |
|------|------|
| ClickHouse 写入延迟 | < 5 秒（窗口闭合后） |
| API 响应时间 | 15-18 ms |
| Flink Checkpoint | 3-4 ms，每 10s |
| Kafka Lag | 稳定，无积压增长 |
| 系统 CPU | ClickHouse ~14%，其余 < 2% |

---

## 目录结构

```
user-behavior-analytics/
├── docker-compose.yml       # 基础设施编排（8 个容器）
├── nginx/                   # Nginx Dockerfile + 配置 + 测试页面
├── filebeat/                # Filebeat 配置（nginx-access-log.yml）
├── db/
│   ├── clickhouse/init/     # ClickHouse 建表 SQL
│   └── mysql/init/          # MySQL 初始化 SQL
├── log-simulator/           # 日志生成器（Python 模拟用户访问）
│   ├── simulator.py         # 常规模拟器 ~3 QPS
│   └── pressure_test.py     # 压力测试脚本 100 QPS
├── flink-job/               # Flink 流计算作业（Maven + Java 17）
│   └── src/main/java/com/analytics/flink/
│       ├── LogAnalysisJob.java      # 作业入口
│       ├── LogParser.java           # 日志解析
│       ├── PvAggregator.java        # PV 聚合
│       ├── UvAggregator.java        # UV 去重聚合（HyperLogLog）
│       ├── StatusAggregator.java    # 状态码聚合
│       ├── RefererAggregator.java   # 来源聚合
│       └── ClickHouseWriter.java    # JDBC 批量写入
├── server/                  # Spring Boot 3 后端（Maven）
│   └── src/main/java/com/analytics/server/
│       ├── controller/AnalyticsController.java  # REST API
│       ├── service/AnalyticsService.java        # ClickHouse 查询 + Redis 缓存
│       ├── service/RealtimePushService.java      # WebSocket 定时推送
│       └── config/WebSocketConfig.java           # STOMP/SockJS 配置
├── ui/                      # Vue 3 + Vite 前端
│   └── src/
│       ├── views/layout/Index.vue     # 大屏布局（5 个 ECharts）
│       ├── views/analysis/            # 5 个分析页面
│       ├── api/websocket.js           # STOMP 客户端
│       ├── stores/realtime.js         # Pinia 状态管理
│       └── router/index.js            # 6 条路由
└── docs/                    # 6 个阶段的设计文档 + 演示脚本
```

---

## 开发路线图

| 阶段 | 内容 | 文档 |
|------|------|------|
| Phase 1 | Docker 基础设施 + Nginx + 测试页面 | [docs](docs/phase-1-infrastructure.md) |
| Phase 2 | Filebeat + Kafka + 日志模拟器 | [docs](docs/phase-2-log-pipeline.md) |
| Phase 3 | Flink 流计算作业（PV/UV/状态码/来源） | [docs](docs/phase-3-flink-job.md) |
| Phase 4 | Spring Boot 3 后端 API + WebSocket | [docs](docs/phase-4-backend.md) |
| Phase 5 | Vue 3 实时大屏 + 5 个分析页面 | [docs](docs/phase-5-frontend.md) |
| Phase 6 | 联调测试 + 压力测试 + 性能优化 | [docs](docs/phase-6-integration.md) |
