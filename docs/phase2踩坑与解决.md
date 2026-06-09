# Phase 2 踩坑记录

## 坑 1：Nginx 日志文件是软链接，Filebeat 无法读取

### 症状

- Filebeat 启动正常，能连接到 Kafka
- 配置的 input 路径 `/var/log/nginx/access.log` 看起来存在
- 但 Filebeat 读到的内容为空（harvester 启动后收到 0 字节）
- `docker compose exec nginx ls -la /var/log/nginx/` 发现：

```
lrwxrwxrwx access.log -> /dev/stdout
lrwxrwxrwx error.log -> /dev/stderr
```

### 原因

Nginx 官方 Docker 镜像在 Dockerfile 中默认把日志文件指向了 stdout/stderr：

```dockerfile
RUN ln -sf /dev/stdout /var/log/nginx/access.log \
    && ln -sf /dev/stderr /var/log/nginx/error.log
```

这样 Nginx 的日志直接输出到控制台（`docker logs` 能看到），适合大多数场景。但在我们的架构中，**Filebeat 需要从一个真实文件中读取**，因为它的工作方式是：

1. 打开文件，记录 inode 和 offset
2. 按行读取，推进 offset
3. 通过 registry 文件持久化进度（即使容器重启也能续读）

对 `/dev/stdout` 做这些操作没有意义——它不是一个可被追踪的实体文件。所以 Filebeat 虽然提示 "Harvester started"，但实际读取不到任何日志行。

### 解决方案

修改 `nginx/Dockerfile`，在构建时删掉软链接，创建真实文件：

```dockerfile
FROM nginx:latest
COPY nginx.conf /etc/nginx/nginx.conf
COPY html /usr/share/nginx/html
# 删除到 /dev/stdout 的软链接，改为真实文件（Filebeat 需要）
RUN rm -f /var/log/nginx/access.log /var/log/nginx/error.log && \
    touch /var/log/nginx/access.log /var/log/nginx/error.log
```

做了两件事：
- `rm -f` — 删掉指向 `/dev/stdout` 和 `/dev/stderr` 的软链接
- `touch` — 创建空的真实文件，供 Filebeat 读取和追踪

### 额外操作

由于 `nginx-logs` 是 Docker 命名卷（named volume），第一次挂载时将镜像中的文件/软链接原样拷贝到了卷中。即使重建镜像，老的卷仍保留着软链接。所以还需要：

```bash
# 停止并删除相关容器
docker compose stop nginx filebeat
docker compose rm -f nginx filebeat
# 删除旧的卷（释放软链接）
docker volume rm user-behavior-analytics_nginx-logs
# 重建 Nginx 镜像
docker compose build nginx
# 重新创建卷和容器
docker compose up -d nginx filebeat
```

---

## 坑 2：Kafka Advertised Listeners 配置错误导致消息卡住

### 症状

- Filebeat 能连接到 Kafka（日志显示 `Connection to kafka(kafka:9092) established`）
- 手动 Kafka 生产/消费测试正常
- 但 Filebeat 的消息发出去后永远收不到 ack：

```
# Filebeat 内部日志指标
output.events.active: 11     # 消息一直"活跃"（未被确认）
output.events.acked: 0       # 从未成功
output.events.failed: 0      # 也没有失败（只是在不断重试）
write.latency.histogram.count: 0  # 没有任何写操作完成
```

更诡异的是 Kafka 那边毫无报错，没有任何错误日志。

### 原因

**根因是 Kafka 的 `KAFKA_ADVERTISED_LISTENERS` 设成了 `PLAINTEXT://localhost:9092`。**

Kafka 的工作机制是：

1. Filebeat 用 `kafka:9092`（Docker DNS 名称）向 Kafka 发起连接
2. Kafka 接受连接，返回 metadata（包含 brokers 列表）
3. metadata 中每个 broker 都有一个 **advertised 地址**，告诉客户端"接下来请用这个地址连接我"
4. **Filebeat 收到 advertised 地址后，断开原连接，改连这个地址**

配置为 `localhost:9092` 时，Kafka 告诉 Filebeat："请连 `localhost:9092`"。但**从 Filebeat 容器内部看**，`localhost` 解析到的是 Filebeat 自己的容器，不是 Kafka 容器。

结果是：
- TCP 连接建立到自身的一个端口（可能连到的是别的进程，或者根本没人监听）
- 数据"发出去了"，但永远不会收到 Kafka 的 ProduceResponse 确认
- Sarama 客户端反复重试，永远卡住

```
┌─────────────────┐                    ┌─────────────────┐
│   Filebeat      │  bootstrap connect  │   Kafka         │
│                  │ ─────────────────→ │                  │
│                  │                    │                  │
│                  │ ← metadata         │                  │
│                  │   "advertised:     │                  │
│                  │    localhost:9092" │                  │
│                  │                    │                  │
│                  │  produce request   │                  │
│                  │ ─────────────────→ │    ┌────────┐   │
│  localhost:9092  │        ✗           │    │  ???   │   │
│  (自己的容器)    │    连到自己         │    └────────┘   │
│                  │                    │                  │
└─────────────────┘                    └─────────────────┘
```

### 为什么之前的命令行工具能用？

因为 `kafka-console-producer` 和 `kafka-console-consumer` 通过 `docker compose exec` 运行，**在 Kafka 容器内部执行**，`localhost` 指向的就是 Kafka 自身，所以没问题。

### 解决方案

采用 **双 listener 模式**：

```yaml
KAFKA_LISTENERS: >
  PLAINTEXT://0.0.0.0:9092,          # 容器间通信(Filebeat → Kafka)
  CONTROLLER://0.0.0.0:9093,          # KRaft 控制器内部通信
  PLAINTEXT_HOST://0.0.0.0:19092     # 容器内 CLI 用

KAFKA_ADVERTISED_LISTENERS: >
  PLAINTEXT://kafka:9092,             # 告诉其他容器：用 Docker DNS 名称
  PLAINTEXT_HOST://localhost:19092    # 告诉本机 CLI：用 localhost:19092
```

- **PLAINTEXT** listener 监听 9092 端口，advertised 地址为 `kafka:9092`（Docker 内部 DNS 名）。Filebeat 等容器通过此地址连接。
- **PLAINTEXT_HOST** listener 监听 19092 端口，advertised 地址为 `localhost:19092`。通过 `docker compose exec` 运行的命令行工具用这个地址。

对应的安全协议映射：
```yaml
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: >
  PLAINTEXT:PLAINTEXT,
  CONTROLLER:PLAINTEXT,
  PLAINTEXT_HOST:PLAINTEXT
```

最终通信流程：

```
┌─────────────────┐                    ┌─────────────────┐
│   Filebeat      │  kafka:9092         │   Kafka         │
│                  │ ─────────────────→ │                  │
│                  │                    │                  │
│                  │ ← metadata         │                  │
│                  │   "advertised:     │                  │
│                  │    kafka:9092"     │                  │
│                  │                    │                  │
│                  │  produce request   │                  │
│                  │ ─────────────────→ │                  │
│                  │  ← ack ✓          │                  │
└─────────────────┘                    └─────────────────┘
```

### 验证方法

修复后，Filebeat 内部日志确认消息已被确认：

```
output.events.acked: 11    # ✓ 消息被 Kafka 确认
output.events.active: 0    # ✓ 无卡住消息
```

从 Kafka 消费到完整 JSON 格式的日志消息：

```json
{
  "message": "192.168.3.181|-|[09/Jun/2026:09:58:42]|\"GET /product/1 HTTP/1.1\"|200|38682|...",
  "fields": {"log_type": "nginx_access"},
  "log": {"offset": 6480, "file": {"path": "/var/log/nginx/access.log"}}
}
```

---

## 通用教训

| 问题 | 本质 | 教训 |
|------|------|------|
| Nginx 日志软链接 | 官方镜像的默认行为与我们的架构需求冲突 | 使用 Docker 镜像时，不要假设默认的文件布局适合你的场景 |
| Kafka advertised listeners | bootstrap 地址 ≠ 实际通信地址 | 分布式系统（Kafka、Cassandra 等）都有 "advertised address" 的双地址模型，容器环境下必须明确配置 |



# 通俗解释

## Phase 2 遇到的两个坑（简单版）

---

### 坑1：Nginx 日志是假的

**现象：** Filebeat 读不到 Nginx 日志

**原因：** 
Nginx 官方镜像的 `access.log` 不是一个真实文件，而是一个指向 `/dev/stdout` 的**软链接**（快捷方式）。日志直接打印到控制台，根本没写硬盘。

**解决：**
```dockerfile
# 删掉软链接，创建真实文件
RUN rm -f /var/log/nginx/access.log && \
    touch /var/log/nginx/access.log
```

**一句话：Nginx 把日志扔给了空气，Filebeat 捡了个寂寞。**

---

### 坑2：Kafka 指错回家的路

**现象：** Filebeat 能连上 Kafka，但发出去的消息卡住，永远收不到确认

**原因：**
Kafka 启动时广播自己的地址：`我在这，地址是 localhost:9092`

- Filebeat 通过 `kafka:9092`（Docker 内部 DNS）连接成功
- 但 Kafka 告诉 Filebeat："你把消息发到 `localhost:9092`"
- 从 Filebeat 容器看，`localhost` 是 Filebeat 自己，不是 Kafka

**结果：** 消息发出去了，但 Kafka 在等 Filebeat 发到 `localhost`，Filebeat 以为发到了 `kafka:9092`——两个鸡同鸭讲。

**解决：**
让 Kafka 同时告诉别人两个地址：
- `kafka:9092` → 供其他容器使用
- `localhost:19092` → 供本机使用

**一句话：Kafka 给 Filebeat 指了条错路（localhost），Filebeat 对着空气喊了半天。**

---

## 总结

| 坑   | 问题                       | 一句话               |
| ---- | -------------------------- | -------------------- |
| 坑1  | Nginx 日志是软链接         | 文件不存在，读个锤子 |
| 坑2  | Kafka 广播地址是 localhost | 指错路，消息白送     |

**都是容器环境的典型问题**——镜像的默认行为和容器网络的服务发现，总有些"你以为"和"实际"不一样的地方。
