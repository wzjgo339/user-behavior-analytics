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

-- 创建分析用户（Flink / 后端远程访问用）
CREATE USER IF NOT EXISTS analytics IDENTIFIED WITH no_password;
GRANT ALL ON *.* TO analytics;
