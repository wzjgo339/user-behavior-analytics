#!/usr/bin/env python3
"""压力测试：模拟高并发访问 Nginx 日志"""

import threading
import time
import random
import sys
from datetime import datetime, timezone

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

IPS = [f"192.168.{random.randint(1, 10)}.{random.randint(1, 255)}" for _ in range(200)]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/125.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Safari/605.1.15",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) Mobile/15E148",
    "Mozilla/5.0 (Linux; Android 14) Mobile Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Edge/125.0.0.0",
]

REFERERS = [
    "https://www.google.com/search?q=test",
    "https://www.baidu.com/s?wd=data",
    "https://social.example.com/share/123",
    "",
    "https://www.bing.com/search?q=analytics",
    "",
]

STATUSES = [(200, 95), (404, 2), (403, 1), (500, 1), (502, 1)]


def random_choice_weighted(items):
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
    status = random_choice_weighted(STATUSES)
    body_bytes = random.randint(200, 50000)
    referer = random.choice(REFERERS)
    ua = random.choice(USER_AGENTS)
    response_time = round(random.uniform(0.005, 0.5), 3)
    return (
        f'{ip}|-|[{now}]|"{url}"|{status}|{body_bytes}|'
        f'"{referer}"|"{ua}"|{response_time}|-'
    )


def write_worker(worker_id, qps, log_file):
    """单个工人以指定 QPS 写入日志"""
    interval = 1.0 / qps
    count = 0
    while True:
        line = generate_log_line()
        try:
            with open(log_file, "a") as f:
                f.write(line + "\n")
        except Exception as e:
            print(f"[Worker {worker_id}] 写入失败: {e}")
        time.sleep(interval)
        count += 1
        if count % 1000 == 0:
            print(f"[Worker {worker_id}] 已写入 {count} 条")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="压力测试")
    parser.add_argument("--workers", type=int, default=4, help="工人线程数")
    parser.add_argument("--qps-per-worker", type=int, default=25, help="每线程 QPS")
    parser.add_argument("--log-file", default="/var/log/nginx/access.log", help="日志文件路径")
    args = parser.parse_args()

    log_file = args.log_file
    total_qps = args.workers * args.qps_per_worker

    print(f"压力测试启动：{args.workers} 个工人 × {args.qps_per_worker} QPS/线程 = {total_qps} QPS")
    print(f"写入目标: {log_file}")
    print("按 Ctrl+C 停止\n")

    threads = []
    for i in range(args.workers):
        t = threading.Thread(target=write_worker, args=(i, args.qps_per_worker, log_file), daemon=True)
        t.start()
        threads.append(t)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n压力测试停止")


if __name__ == "__main__":
    main()
