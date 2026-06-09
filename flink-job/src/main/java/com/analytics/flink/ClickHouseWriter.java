package com.analytics.flink;

import com.analytics.flink.model.LogEvent;
import org.apache.flink.api.common.functions.MapFunction;

import java.sql.*;

/**
 * ClickHouse 写入器
 * 每个 Writer 类执行 INSERT 操作
 * 测试环境每个请求建一个新连接，生产环境应改为批量写入（每 10s 或每 1000 条 flush 一次）
 */
public class ClickHouseWriter {

    // ClickHouse 新版（26.x）限制 default 用户仅可 localhost 访问
    // 使用 docker-entrypoint-initdb.d 中创建的 analytics 用户连接（无密码）
    private static final String JDBC_URL = "jdbc:clickhouse://clickhouse:8123/default?user=analytics";
    private static final String DRIVER = "com.clickhouse.jdbc.ClickHouseDriver";

    static {
        try {
            Class.forName(DRIVER);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("ClickHouse JDBC Driver not found", e);
        }
    }

    private static Connection getConn() throws SQLException {
        return DriverManager.getConnection(JDBC_URL);
    }

    // ========== PV 分钟写入 ==========
    public static class PvMinuteWriter implements MapFunction<PvAggregator.PvResult, String> {
        @Override
        public String map(PvAggregator.PvResult r) throws Exception {
            String sql = "INSERT INTO pv_minute (window_start, window_end, url, pv_count, " +
                    "status_2xx, status_3xx, status_4xx, status_5xx, avg_response_time) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
            try (Connection conn = getConn();
                 PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setTimestamp(1, new Timestamp(r.windowStart));
                ps.setTimestamp(2, new Timestamp(r.windowEnd));
                ps.setString(3, r.url);
                ps.setLong(4, r.count);
                ps.setLong(5, r.status2xx);
                ps.setLong(6, r.status3xx);
                ps.setLong(7, r.status4xx);
                ps.setLong(8, r.status5xx);
                ps.setDouble(9, r.avgResponseTime);
                ps.executeUpdate();
            }
            return "ok";
        }
    }

    // ========== UV 分钟写入 ==========
    public static class UvMinuteWriter implements MapFunction<UvAggregator.UvResult, String> {
        @Override
        public String map(UvAggregator.UvResult r) throws Exception {
            String sql = "INSERT INTO uv_minute (window_start, window_end, uv_count) VALUES (?, ?, ?)";
            try (Connection conn = getConn();
                 PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setTimestamp(1, new Timestamp(r.windowStart));
                ps.setTimestamp(2, new Timestamp(r.windowEnd));
                ps.setLong(3, r.uvCount);
                ps.executeUpdate();
            }
            return "ok";
        }
    }

    // ========== 来源分析写入（每小时）==========
    public static class RefererHourlyWriter implements MapFunction<RefererAggregator.RefererResult, String> {
        @Override
        public String map(RefererAggregator.RefererResult r) throws Exception {
            String sql = "INSERT INTO referer_hourly (window_start, referer_type, count) VALUES (?, ?, ?)";
            try (Connection conn = getConn();
                 PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setTimestamp(1, new Timestamp(r.windowStart));
                ps.setString(2, r.refererType);
                ps.setLong(3, r.count);
                ps.executeUpdate();
            }
            return "ok";
        }
    }

    // ========== 状态码分布写入（每分钟）==========
    public static class StatusMinuteWriter implements MapFunction<StatusAggregator.StatusResult, String> {
        @Override
        public String map(StatusAggregator.StatusResult r) throws Exception {
            String sql = "INSERT INTO status_minute (window_start, status_code, count) VALUES (?, ?, ?)";
            try (Connection conn = getConn();
                 PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setTimestamp(1, new Timestamp(r.windowStart));
                ps.setInt(2, r.statusCode);
                ps.setLong(3, r.count);
                ps.executeUpdate();
            }
            return "ok";
        }
    }

    // ========== 采样日志写入 ==========
    public static class LogSampleWriter implements MapFunction<LogEvent, String> {
        @Override
        public String map(LogEvent e) throws Exception {
            String sql = "INSERT INTO access_log_sample (timestamp, ip, url, status, " +
                    "response_time, referer, user_agent) VALUES (?, ?, ?, ?, ?, ?, ?)";
            try (Connection conn = getConn();
                 PreparedStatement ps = conn.prepareStatement(sql)) {
                // 将 LocalDateTime 转为 Timestamp
                ps.setTimestamp(1, Timestamp.valueOf(e.timestamp));
                ps.setString(2, e.ip);
                ps.setString(3, e.url);
                ps.setInt(4, e.status);
                ps.setFloat(5, (float) e.responseTime);
                ps.setString(6, e.referer);
                ps.setString(7, e.userAgent);
                ps.executeUpdate();
            }
            return "ok";
        }
    }
}
