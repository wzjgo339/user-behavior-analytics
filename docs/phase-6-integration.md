# Phase 6：联调测试与优化

## 目标

端到端验证全链路，进行压力测试和性能优化，输出可演示的完整系统。

---

## 步骤 6.1：端到端验证

按以下顺序从数据源到展示面逐层验证：

```bash
# 1. 确认所有 Docker 容器运行
docker compose ps

# 2. 确认 Nginx 可访问并产生日志
curl http://localhost
docker compose exec nginx cat /var/log/nginx/access.log | tail -3

# 3. 确认 Filebeat 成功发送到 Kafka
docker compose logs filebeat --tail 20 | grep -i "kafka"

# 4. 确认 Kafka 有消息
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic nginx-access-log --from-beginning --max-messages 5

# 5. 确认 Flink 作业运行
docker exec flink-jobmanager flink list

# 6. 确认 ClickHouse 有数据
docker compose exec clickhouse clickhouse-client \
  --query "SELECT count() FROM pv_minute"

# 7. 确认后端 API 返回数据
curl http://localhost:8080/api/overview

# 8. 确认前端展示
# 打开 http://localhost:3000 查看大屏
```

### 常见问题排查

| 现象 | 排查方向 |
|------|---------|
| 大屏数据为 0 | 日志模拟器是否运行？→ Filebeat 是否报错？→ Kafka 是否有消息？→ Flink 是否写入 ClickHouse？ |
| WebSocket 连不上 | 检查后端是否已启动 → 浏览器控制台看 WebSocket 错误 → 检查 CORS 配置 |
| 图表不更新 | 确认 store.update 被调用 → data 结构是否匹配 |
| Nginx 日志为空 | 确认 `access_log` 指令启用 → 确认日志路径挂载正确 |
| ClickHouse 查询慢 | 见步骤 6.3 优化方案 |

---

## 步骤 6.2：压力测试

### 2.1 使用模拟器加压

修改日志模拟器的间隔时间和并发：

```python
# log-simulator/pressure_test.py
"""压力测试：模拟高并发访问"""

import threading
import time
from simulator import generate_log_line, log_file

def write_worker(worker_id, qps):
    """单个工人以指定 QPS 写入日志"""
    interval = 1.0 / qps
    while True:
        line = generate_log_line()
        try:
            with open(log_file, "a") as f:
                f.write(line + "\n")
        except Exception as e:
            print(f"[Worker {worker_id}] 写入失败: {e}")
        time.sleep(interval)

def main():
    # 模拟 100 QPS（4 个工人，每个 25 QPS）
    workers = 4
    qps_per_worker = 25
    for i in range(workers):
        t = threading.Thread(target=write_worker, args=(i, qps_per_worker), daemon=True)
        t.start()
    print(f"压力测试启动：{workers * qps_per_worker} QPS")
    time.sleep(999999)

if __name__ == "__main__":
    main()
```

### 2.2 观察组件表现

```bash
# Kafka 消息积压情况
docker compose exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group flink-analytics --describe

# ClickHouse 写入速度
docker compose exec clickhouse clickhouse-client \
  --query "SELECT count() FROM pv_minute"

# 后端 API 响应时间
time curl http://localhost:8080/api/overview

# 系统资源占用
docker stats
```

### 2.3 注意观察项

- ClickHouse 写入延迟是否在合理范围（秒级以内）
- Kafka 消费者 Lag 是否持续增长（增长说明处理速度跟不上）
- 后端 API 响应时间是否超过 500ms
- 前端 WebSocket 推送是否卡顿

---

## 步骤 6.3：性能优化

### 3.1 ClickHouse 优化

```sql
-- 1. 创建物化视图，按小时预聚合（避免对大表重复查询）
CREATE MATERIALIZED VIEW pv_hourly_mv
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMMDD(hour)
ORDER BY (hour, url)
AS SELECT
    toStartOfHour(window_start) as hour,
    url,
    sum(pv_count) as total_pv,
    avg(avg_response_time) as avg_rt
FROM pv_minute
GROUP BY hour, url;

-- 查询时直接查物化视图，速度快 10-100 倍
SELECT * FROM pv_hourly_mv WHERE hour >= now() - INTERVAL 1 DAY;

-- 2. 添加跳数索引（适合高基数 URL 字段）
ALTER TABLE pv_minute ADD INDEX url_bloom (url) TYPE bloom_filter(0.01) GRANULARITY 4;

-- 3. 优化分区策略（每天一个分区，查询带上分区条件）
-- 已经 PARTITION BY toYYYYMMDD(window_start)
```

### 3.2 Flink 优化

```java
// 1. 开启 Checkpoint（故障恢复）
env.enableCheckpointing(60_000); // 每分钟

// 2. 批量写入 ClickHouse（减少连接开销）
// 在 ClickHouseWriter 中添加 flush 机制：
// 每 1000 条或 10 秒 flush 一次
// 使用 JDBC 的 batch insert
```

### 3.3 后端优化

```java
// 1. 添加 Redis 缓存（减少重复查询）
// 在 AnalyticsService 中修改：

@Autowired
private StringRedisTemplate redisTemplate;

public long getTodayPv() {
    String cached = redisTemplate.opsForValue().get("analytics:pv");
    if (cached != null) return Long.parseLong(cached);

    long pv = // 查询 ClickHouse
    redisTemplate.opsForValue().set("analytics:pv", String.valueOf(pv), 30, TimeUnit.SECONDS);
    return pv;
}

// 2. WebSocket 推送频率可动态调节
// 正常情况下 5 秒，高峰期改为 10 秒
```

### 3.4 前端优化

```javascript
// 1. 减少图表重绘频率（数据无变化时不刷新）
// 2. 使用 requestAnimationFrame 替代 setInterval
// 3. 离屏时暂停动画
document.addEventListener('visibilitychange', () => {
    if (document.hidden) {
        // 暂停 WebSocket / 停止图表刷新
    } else {
        // 恢复
    }
})
```

---

## 步骤 6.4：生产化检查清单

### 安全性
- [ ] 后端 API 添加认证（Spring Security / JWT）
- [ ] WebSocket 连接鉴权
- [ ] ClickHouse 和 Kafka 不暴露到公网
- [ ] Nginx 配置 HTTPS

### 可靠性
- [ ] Docker 容器设置 `restart: always`
- [ ] Flink 开启 Checkpoint
- [ ] 日志轮转（防止磁盘写满）
- [ ] 资源限制（`deploy.resources.limits`）

### 可观测性
- [ ] 后端添加健康检查接口（已实现 `/api/health`）
- [ ] 关键组件暴露 Prometheus 指标
- [ ] 错误日志统一收集

---

## 步骤 6.5：演示脚本

### 演示流程（约 5 分钟）

```
1. 启动全部服务
   docker compose up -d && cd server && mvn spring-boot:run

2. 启动日志模拟器
   python3 log-simulator/simulator.py

3. 打开实时大屏
   http://localhost:3000

4. 依次介绍：
   a) 整体架构（数据流图）
   b) 实时大屏 → 数字跳动、图表动态更新
   c) PV 分析页面 → 历史趋势、时间范围选择
   d) 来源分析 → 各渠道占比
   e) 性能监控 → 错误率告警

5. 演示弹性
   - 停止模拟器再启动 → 数据恢复
   - 展示 ClickHouse 查询速度
```

---

## 本阶段完成标志

- [ ] 全链路验证通过（Nginx ← ClickHouse → API → 前端）
- [ ] 压力测试 100 QPS 下各组件稳定
- [ ] ClickHouse 查询 < 100ms
- [ ] WebSocket 推送无明显延迟
- [ ] 大屏展示效果流畅
- [ ] 演示流程可完整走通
