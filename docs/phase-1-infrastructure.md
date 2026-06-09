# Phase 1：基础设施（Docker 化）

## 目标

部署 Kafka + ClickHouse + Redis + MySQL + Nginx，所有组件通过 Docker Compose 一键启动，网络互通。

---

## 步骤 1.1：创建项目根目录

```bash
mkdir -p user-behavior-analytics
cd user-behavior-analytics
```

后续所有操作都在此目录下执行。

---

## 步骤 1.2：编写 docker-compose.yml

创建 `docker-compose.yml`：

```yaml
version: "3.8"

services:
  # ===== 消息队列 =====
  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    container_name: zk
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
    networks: [analytics-net]

  kafka:
    image: confluentinc/cp-kafka:latest
    container_name: kafka
    depends_on: [zookeeper]
    ports: ["9092:9092"]
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    networks: [analytics-net]

  # ===== 分析数据库 =====
  clickhouse:
    image: clickhouse/clickhouse-server:latest
    container_name: clickhouse
    ports: ["8123:8123", "9000:9000"]
    volumes:
      - ./db/clickhouse/init:/docker-entrypoint-initdb.d
      - clickhouse-data:/var/lib/clickhouse
    networks: [analytics-net]

  # ===== 缓存 =====
  redis:
    image: redis:7-alpine
    container_name: redis
    ports: ["6379:6379"]
    networks: [analytics-net]

  # ===== 业务数据库 =====
  mysql:
    image: mysql:8.0
    container_name: mysql
    ports: ["3306:3306"]
    environment:
      MYSQL_ROOT_PASSWORD: root123
      MYSQL_DATABASE: analysis
    volumes:
      - ./db/mysql/init:/docker-entrypoint-initdb.d
      - mysql-data:/var/lib/mysql
    networks: [analytics-net]

  # ===== Web 服务器 =====
  nginx:
    build: ./nginx
    container_name: nginx
    ports: ["80:80"]
    volumes:
      - nginx-logs:/var/log/nginx
    networks: [analytics-net]

  # ===== 日志采集器（依赖 Nginx 和 Kafka 就绪）=====
  filebeat:
    image: docker.elastic.co/beats/filebeat:8.14.0
    container_name: filebeat
    user: root
    depends_on: [nginx, kafka]
    volumes:
      - ./filebeat/filebeat.yml:/usr/share/filebeat/filebeat.yml:ro
      - nginx-logs:/var/log/nginx:ro
    networks: [analytics-net]

networks:
  analytics-net:
    driver: bridge

volumes:
  clickhouse-data:
  mysql-data:
  nginx-logs:
```

---

## 步骤 1.3：创建 Nginx 配置

### 创建目录

```bash
mkdir -p nginx html
```

### nginx/Dockerfile

```dockerfile
FROM nginx:latest
COPY nginx.conf /etc/nginx/nginx.conf
COPY html /usr/share/nginx/html
RUN mkdir -p /var/log/nginx
```

### nginx/nginx.conf

```nginx
worker_processes auto;
error_log /var/log/nginx/error.log;

events {
    worker_connections 1024;
}

http {
    log_format analysis '$remote_addr|$remote_user|[$time_local]|'
                        '"$request"|$status|$body_bytes_sent|'
                        '"$http_referer"|"$http_user_agent"|'
                        '$request_time|$upstream_addr';

    access_log /var/log/nginx/access.log analysis;

    include /etc/nginx/mime.types;
    default_type application/octet-stream;
    sendfile on;
    keepalive_timeout 65;

    server {
        listen 80;
        server_name localhost;

        # 测试页面
        location / {
            root /usr/share/nginx/html;
            index index.html;
        }

        # API 代理（Phase 4 后启用）
        # location /api/ {
        #     proxy_pass http://server:8080/;
        # }

        # 静态资源
        location /static/ {
            root /usr/share/nginx/html;
        }
    }
}
```

### nginx/html/index.html（测试页）

```html
<!DOCTYPE html>
<html>
<head><title>用户行为分析平台</title></head>
<body>
    <h1>用户行为分析平台</h1>
    <p>实时数据分析系统运行中...</p>
    <ul>
        <li><a href="/">首页</a></li>
        <li><a href="/product/1">商品A</a></li>
        <li><a href="/product/2">商品B</a></li>
        <li><a href="/cart">购物车</a></li>
        <li><a href="/checkout">结算</a></li>
        <li><a href="/about">关于我们</a></li>
    </ul>
</body>
</html>
```

同时创建额外页面，让访问路径更丰富：

```bash
mkdir -p nginx/html/product
```

`nginx/html/product/1.html`：
```html
<!DOCTYPE html><html><head><title>商品A</title></head>
<body><h1>商品A - 详情</h1><a href="/cart">加入购物车</a> | <a href="/">返回首页</a></body></html>
```

类似地创建 `product/2.html`、`cart.html`、`checkout.html`、`about.html`。

---

## 步骤 1.4：创建 ClickHouse 初始化 SQL

```bash
mkdir -p db/clickhouse/init
```

`db/clickhouse/init/01-create-tables.sql`：

```sql
-- 每分钟 URL 访问量聚合
CREATE TABLE IF NOT EXISTS pv_minute (
    window_start   DateTime,
    window_end     DateTime,
    url            String,
    pv_count       UInt64,
    status_2xx     UInt64 DEFAULT 0,
    status_3xx     UInt64 DEFAULT 0,
    status_4xx     UInt64 DEFAULT 0,
    status_5xx     UInt64 DEFAULT 0,
    avg_response_time Float64 DEFAULT 0
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (window_start, url);

-- 每分钟 UV
CREATE TABLE IF NOT EXISTS uv_minute (
    window_start   DateTime,
    window_end     DateTime,
    uv_count       UInt64
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(window_start)
ORDER BY window_start;

-- 每小时来源分析
CREATE TABLE IF NOT EXISTS referer_hourly (
    window_start   DateTime,
    referer_type   String,
    count          UInt64
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (window_start, referer_type);

-- 状态码分布（分钟级）
CREATE TABLE IF NOT EXISTS status_minute (
    window_start   DateTime,
    status_code    UInt16,
    count          UInt64
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (window_start, status_code);

-- 漏斗分析结果
CREATE TABLE IF NOT EXISTS funnel_analysis (
    window_start    DateTime,
    step_order      UInt8,
    step_name       String,
    user_count      UInt64,
    conversion_rate Float32
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (window_start, step_order);

-- 访问日志采样（仅保留 7 天）
CREATE TABLE IF NOT EXISTS access_log_sample (
    timestamp      DateTime,
    ip             String,
    url            String,
    status         UInt16,
    response_time  Float32,
    referer        String,
    user_agent     String
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (timestamp, url)
TTL timestamp + INTERVAL 7 DAY;
```

---

## 步骤 1.5：创建 MySQL 初始化 SQL

```bash
mkdir -p db/mysql/init
```

`db/mysql/init/01-init.sql`：

```sql
CREATE DATABASE IF NOT EXISTS analysis DEFAULT CHARACTER SET utf8mb4;

USE analysis;

-- 系统用户表（登录用）
CREATE TABLE IF NOT EXISTS sys_user (
    user_id     BIGINT AUTO_INCREMENT PRIMARY KEY,
    username    VARCHAR(50) NOT NULL UNIQUE,
    password    VARCHAR(100) NOT NULL,
    nickname    VARCHAR(50),
    email       VARCHAR(100),
    status      CHAR(1) DEFAULT '0',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- 插入默认管理员
INSERT INTO sys_user (username, password, nickname) VALUES
('admin', '$2a$10$7JB720yubVSZvUI0rEqK/.VqGOZTH.ulu33dHOiBEzByNZdLUMs42', '管理员')
ON DUPLICATE KEY UPDATE username=username;
-- 密码是 admin123 的 BCrypt 哈希，后续可用 Spring Security 校验

-- 菜单表
CREATE TABLE IF NOT EXISTS sys_menu (
    menu_id     BIGINT AUTO_INCREMENT PRIMARY KEY,
    menu_name   VARCHAR(50) NOT NULL,
    parent_id   BIGINT DEFAULT 0,
    path        VARCHAR(200),
    component   VARCHAR(200),
    perms       VARCHAR(100),
    icon        VARCHAR(50),
    sort        INT DEFAULT 0,
    visible     CHAR(1) DEFAULT '0',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- 插入基础菜单
INSERT INTO sys_menu (menu_name, parent_id, path, component, perms, icon, sort) VALUES
('实时大屏', 0, '/dashboard', 'dashboard/index', 'dashboard:view', 'monitor', 1),
('PV分析',   0, '/analysis/pv', 'analysis/pv', 'analysis:pv:view', 'chart', 2),
('UV分析',   0, '/analysis/uv', 'analysis/uv', 'analysis:uv:view', 'chart', 3),
('来源分析', 0, '/analysis/referer', 'analysis/referer', 'analysis:referer:view', 'search', 4),
('性能监控', 0, '/analysis/performance', 'analysis/performance', 'analysis:performance:view', 'speed', 5),
('漏斗分析', 0, '/analysis/funnel', 'analysis/funnel', 'analysis:funnel:view', 'funnel', 6);
```

---

## 步骤 1.6：创建 Filebeat 配置（目录 + 占位文件）

```bash
mkdir -p filebeat
```

`filebeat/filebeat.yml`（现在只需骨架，Phase 2 补充完整）：

```yaml
filebeat.inputs:
  - type: log
    enabled: false  # Phase 2 开启
    paths:
      - /var/log/nginx/access.log
    fields:
      log_type: nginx_access

output.kafka:
  enabled: false
  hosts: ["kafka:9092"]
  topic: "nginx-access-log"
```

---

## 步骤 1.7：创建 .gitignore

`.gitignore`：

```
node_modules/
target/
dist/
*.jar
*.log
clickhouse-data/
mysql-data/
.env
```

---

## 步骤 1.8：启动验证

```bash
docker compose up -d

# 检查所有容器运行
docker compose ps

# 验证 Nginx
curl http://localhost

# 验证 ClickHouse
docker compose exec clickhouse clickhouse-client --query "SELECT 1"

# 验证 MySQL
docker compose exec mysql mysql -uroot -proot123 analysis -e "SELECT 1"

# 验证 Redis
docker compose exec redis redis-cli PING

# 验证 Kafka
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

所有命令返回正常即基础设施就绪。

---

## 本阶段完成标志

- [ ] `docker compose ps` 显示所有容器为 `Up` 状态
- [ ] `curl http://localhost` 返回测试页面 HTML
- [ ] ClickHouse 中 `pb_minute` 等表已创建 (`SHOW TABLES`)
- [ ] MySQL 中 `sys_user` 表已创建
- [ ] Kafka 无报错日志
