# 用户行为实时分析平台

## 项目概述

基于 Nginx 访问日志的实时用户行为分析系统。采集 Nginx 日志，通过实时流处理计算 PV/UV/来源/漏斗等指标，最终以实时大屏展示。

## 新旧架构对比

```
旧版：Nginx → Flume → HDFS → Spring Boot 2 → Vue 2
新版：Nginx → Filebeat → Kafka → Flink → ClickHouse → Spring Boot 3 → Vue 3 + WebSocket 大屏
```

## 技术栈

| 层 | 技术 |
|----|------|
| 反向代理 | Nginx (Docker) |
| 日志采集 | Filebeat 8.x (Docker) |
| 消息队列 | Kafka 3.x (Docker) |
| 流处理 | Flink 1.19+ (Docker) |
| 分析存储 | ClickHouse (Docker) |
| 缓存 | Redis 7 (Docker) |
| 关系库 | MySQL 8 (Docker) |
| 后端 | Spring Boot 3 + JDK 17 |
| 前端 | Vue 3 + Vite + ECharts |
| 构建 | Maven 3.9+ / pnpm |

## 数据流

```
用户 → Nginx(access.log) → Filebeat → Kafka → Flink(实时聚合) → ClickHouse
                                                                    ↓
用户 ← Vue大屏(WebSocket) ← Spring Boot 3(REST + WS) ←─────────────┘
```

## 目录结构

```
user-behavior-analytics/
├── docker-compose.yml        # 基础设施（Kafka/ClickHouse/Redis/MySQL/Nginx/Filebeat）
├── nginx/                    # Nginx Dockerfile + 配置 + 测试页面
├── filebeat/                 # Filebeat 配置
├── db/                       # SQL 初始化脚本
│   ├── clickhouse/init/
│   └── mysql/init/
├── log-simulator/            # 日志生成器（Python）
├── flink-job/                # Flink 实时计算（Maven）
├── server/                   # Spring Boot 后端（Maven）
└── ui/                       # Vue 3 前端
```

## 实施路线图（六阶段）

| Phase | 内容 | 依赖 | 预计工时 |
|-------|------|------|---------|
| [1](docs/phase-1-infrastructure.md) | Docker 基础设施 + Nginx | 无 | ★★★ |
| [2](docs/phase-2-log-pipeline.md) | Filebeat + Kafka + 日志模拟 | Phase 1 | ★★ |
| [3](docs/phase-3-flink-job.md) | Flink 实时计算任务 | Phase 2 | ★★★★ |
| [4](docs/phase-4-backend.md) | Spring Boot API + WebSocket | Phase 1（ClickHouse 就绪即可） | ★★★ |
| [5](docs/phase-5-frontend.md) | Vue 3 实时大屏 | Phase 4 | ★★★ |
| [6](docs/phase-6-integration.md) | 联调 + 测试 + 优化 | Phase 3+5 | ★★ |

**推荐执行顺序**：Phase 1 → Phase 2 → Phase 4 → Phase 3 → Phase 5 → Phase 6

## 环境准备

- Docker Desktop（WSL2，分配 ≥ 4GB 内存）
- JDK 17+
- Maven 3.9+
- Node.js 18+
- Git

> 每次只读当前 Phase 对应的文档，完成后再读下一个。
