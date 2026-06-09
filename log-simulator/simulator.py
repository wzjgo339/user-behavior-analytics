#!/usr/bin/env python3
"""
Nginx 访问日志模拟器
随机生成用户访问，追加到 Nginx 容器的 access.log
"""

import random
import time
from datetime import datetime, timezone

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
    now = datetime.now(timezone.utc).strftime("%d/%b/%Y:%H:%M:%S %z")
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
