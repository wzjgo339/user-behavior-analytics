# Phase 2：日志采集管道

## 目标

配置 Filebeat 实时采集 Nginx 日志并发送到 Kafka，同时实现日志模拟器产生测试数据。

---

## 步骤 2.1：完整配置 Filebeat

编辑 `filebeat/filebeat.yml`（覆盖 Phase 1 的占位文件）：

```yaml
filebeat.inputs:
  - type: log
    enabled: true
    paths:
      - /var/log/nginx/access.log
    fields:
      log_type: nginx_access
    # 从头读取（首次调试用，稳定后去掉）
    tail_files: false

output.kafka:
  enabled: true
  hosts: ["kafka:9092"]
  topic: "nginx-access-log"
  partition.round_robin:
    reachable_only: true
  required_acks: 1
  compression: gzip
  max_message_bytes: 1048576

# 避免 Filebeat 自己发监控数据到 es
monitoring.enabled: false

# 日志记录（方便排查）
logging.level: info
logging.to_files: true
logging.files:
  path: /var/log/filebeat
  name: filebeat.log
```

---

## 步骤 2.2：在 Kafka 中创建 Topic

```bash
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --topic nginx-access-log --partitions 3 --replication-factor 1

docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --topic nginx-aggregated --partitions 3 --replication-factor 1

# 确认 topic 已创建
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

---

## 步骤 2.3：重启 Filebeat 使配置生效

```bash
docker compose restart filebeat

# 查看 Filebeat 日志，确认无报错
docker compose logs filebeat
```

---

## 步骤 2.4：编写日志模拟器

创建一个 Python 脚本，模拟用户访问 Nginx 产生日志。

`log-simulator/simulator.py`：

```python
#!/usr/bin/env python3
"""
Nginx 访问日志模拟器
随机生成用户访问，追加到 Nginx 容器的 access.log
"""

import random
import time
from datetime import datetime

# 模拟 URL 池（带权重）
URLS = [
    ("GET /index HTTP/1.1", 100),
    ("GET /product/1 HTTP/1.1", 60),
    ("GET /product/2 HTTP/1.1", 55),
    ("GET /cart HTTP/1.1", 30),
    ("GET /checkout HTTP/1.1", 15),
    ("GET /about HTTP/1.1", 10),
    ("POST /api/login HTTP/1.1", 20),
    ("POST /api/search?q=手机 HTTP/1.1", 25),
]

# IP 池（模拟不同用户）
IPS = [
    f"192.168.{random.randint(1, 10)}.{random.randint(1, 255)}"
    for _ in range(50)
]

# User-Agent 池
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/125.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Safari/605.1.15",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) Mobile/15E148",
    "Mozilla/5.0 (Linux; Android 14) Mobile Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Edge/125.0.0.0",
]

# Referer 池
REFERERS = [
    "https://www.google.com/search?q=test",
    "https://www.baidu.com/s?wd=data",
    "https://social.example.com/share/123",
    "",  # 直接访问
    "https://www.bing.com/search?q=analytics",
    "",  # 直接访问
]


def random_choice_weighted(items):
    """按权重随机选择"""
    total = sum(w for _, w in items)
    r = random.uniform(0, total)
    upto = 0
    for item, weight in items:
        upto += weight
        if r <= upto:
            return item
    return items[-1][0]


def generate_log_line():
    now = datetime.now().strftime("%d/%b/%Y:%H:%M:%S %z")
    ip = random.choice(IPS)
    url = random_choice_weighted(URLS)

    # 5% 概率产生 4xx/5xx 状态码
    rand = random.random()
    if rand < 0.03:
        status = random.choice([404, 403])
    elif rand < 0.05:
        status = random.choice([500, 502, 503])
    else:
        status = 200

    body_bytes = random.randint(200, 50000)
    referer = random.choice(REFERERS)
    ua = random.choice(USER_AGENTS)
    response_time = round(random.uniform(0.005, 0.5), 3)

    return (
        f'{ip}|-|[{now}]|"{url}"|{status}|{body_bytes}|'
        f'"{referer}"|"{ua}"|{response_time}|-'
    )


def main():
    log_file = "/var/log/nginx/access.log"
    print(f"模拟器启动，写入 {log_file}")
    print("按 Ctrl+C 停止")

    # 如果是 Docker 外运行，需要确认日志文件可写
    try:
        with open(log_file, "a") as f:
            while True:
                line = generate_log_line()
                f.write(line + "\n")
                f.flush()
                # 随机间隔 0.1~2 秒，模拟真实访问
                time.sleep(random.uniform(0.1, 2.0))
    except KeyboardInterrupt:
        print("\n模拟器停止")
    except PermissionError:
        print(f"错误：无法写入 {log_file}")
        print("请确认路径正确，Docker 环境用：")
        print("  docker compose exec nginx sh")
        print("  然后在容器内运行此脚本，或挂载卷后从宿主机写入")


if __name__ == "__main__":
    main()
```

### 运行方式（二选一）

**方式 A：在容器内运行（推荐）**

```bash
# 将模拟器复制到 Nginx 容器内运行
docker compose cp log-simulator/simulator.py nginx:/simulator.py
docker compose exec nginx sh -c "apk add --no-cache python3 && python3 /simulator.py"
```

**方式 B：通过挂载卷从宿主机写入**

修改 `docker-compose.yml` 的 Nginx 服务，将日志映射到宿主机：

```yaml
  nginx:
    volumes:
      - ./nginx/logs:/var/log/nginx   # 替换 nginx-logs 命名卷
```

然后运行：

```bash
mkdir -p nginx/logs
python3 log-simulator/simulator.py
```

---

## 步骤 2.5：验证管道连通

```bash
# 启动模拟器后，开另一个终端消费 Kafka 消息确认
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic nginx-access-log \
  --from-beginning \
  --max-messages 10
```

如果能看到格式如下的日志行，管道就通了：

```
192.168.3.42|-|[09/Jun/2026:14:30:15 +0800]|"GET /index HTTP/1.1"|200|1234|""|"Mozilla/5.0..."|0.032|-
```

---

## 本阶段完成标志

- [ ] Filebeat 日志无报错（`docker compose logs filebeat`）
- [ ] Kafka topic `nginx-access-log` 已创建
- [ ] 日志模拟器正常运行
- [ ] Kafka 消费者能收到日志消息
- [ ] 消息内容完整的包含 IP、URL、状态码、User-Agent、响应时间
