package com.analytics.server.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * ClickHouse 查询服务
 * 提供各类分析指标的查询方法，使用 Redis 缓存减少重复查询
 */
@Service
public class AnalyticsService {

    private final JdbcTemplate clickhouseJdbc;
    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;

    private static final long CACHE_SHORT = 10;   // 10 秒
    private static final long CACHE_LONG = 30;    // 30 秒

    public AnalyticsService(DataSource clickhouseDataSource,
                            StringRedisTemplate redisTemplate,
                            ObjectMapper objectMapper) {
        this.clickhouseJdbc = new JdbcTemplate(clickhouseDataSource);
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
    }

    /** 今日总 PV（缓存 30 秒） */
    public long getTodayPv() {
        String cacheKey = "analytics:pv";
        String cached = redisTemplate.opsForValue().get(cacheKey);
        if (cached != null) {
            return Long.parseLong(cached);
        }
        String sql = "SELECT sum(pv_count) FROM pv_minute " +
                     "WHERE window_start >= toDate(now())";
        Long result = clickhouseJdbc.queryForObject(sql, Long.class);
        long pv = result != null ? result : 0;
        redisTemplate.opsForValue().set(cacheKey, String.valueOf(pv), CACHE_LONG, TimeUnit.SECONDS);
        return pv;
    }

    /** 今日总 UV（缓存 30 秒） */
    public long getTodayUv() {
        String cacheKey = "analytics:uv";
        String cached = redisTemplate.opsForValue().get(cacheKey);
        if (cached != null) {
            return Long.parseLong(cached);
        }
        String sql = "SELECT sum(uv_count) FROM uv_minute " +
                     "WHERE window_start >= toDate(now())";
        Long result = clickhouseJdbc.queryForObject(sql, Long.class);
        long uv = result != null ? result : 0;
        redisTemplate.opsForValue().set(cacheKey, String.valueOf(uv), CACHE_LONG, TimeUnit.SECONDS);
        return uv;
    }

    /** TOP N 热门页面（缓存 30 秒） */
    public List<Map<String, Object>> getTopPages(int limit) {
        String cacheKey = "analytics:topPages:" + limit;
        String cached = redisTemplate.opsForValue().get(cacheKey);
        if (cached != null) {
            try {
                return objectMapper.readValue(cached, new TypeReference<>() {});
            } catch (JsonProcessingException e) {
                // 缓存解析失败，回退查询
            }
        }
        String sql = "SELECT url, sum(pv_count) as pv FROM pv_minute " +
                     "WHERE window_start >= toDate(now()) " +
                     "GROUP BY url ORDER BY pv DESC LIMIT ?";
        List<Map<String, Object>> result = clickhouseJdbc.queryForList(sql, limit);
        try {
            redisTemplate.opsForValue().set(cacheKey, objectMapper.writeValueAsString(result),
                    CACHE_LONG, TimeUnit.SECONDS);
        } catch (JsonProcessingException e) {
            // 序列化失败不影响主流程
        }
        return result;
    }

    /** 状态码分布（最近 5 分钟，缓存 10 秒） */
    public Map<String, Long> getStatusDistribution() {
        String cacheKey = "analytics:statusCodes";
        String cached = redisTemplate.opsForValue().get(cacheKey);
        if (cached != null) {
            try {
                return objectMapper.readValue(cached, new TypeReference<>() {});
            } catch (JsonProcessingException e) {
                // 缓存解析失败，回退查询
            }
        }
        String sql = "SELECT toString(intDiv(status_code, 100)) || 'xx' as code_range, " +
                     "       sum(count) as cnt " +
                     "FROM status_minute " +
                     "WHERE window_start >= now() - INTERVAL 5 MINUTE " +
                     "GROUP BY code_range";
        List<Map<String, Object>> rows = clickhouseJdbc.queryForList(sql);
        Map<String, Long> result = new HashMap<>();
        for (Map<String, Object> row : rows) {
            String range = (String) row.get("code_range");
            Number cnt = (Number) row.get("cnt");
            result.put(range != null ? range : "unknown", cnt != null ? cnt.longValue() : 0L);
        }
        result.putIfAbsent("2xx", 0L);
        result.putIfAbsent("3xx", 0L);
        result.putIfAbsent("4xx", 0L);
        result.putIfAbsent("5xx", 0L);
        try {
            redisTemplate.opsForValue().set(cacheKey, objectMapper.writeValueAsString(result),
                    CACHE_SHORT, TimeUnit.SECONDS);
        } catch (JsonProcessingException e) {
            // 序列化失败不影响主流程
        }
        return result;
    }

    /** 最近 N 分钟的 PV 时间序列 */
    public List<Map<String, Object>> getPvTrend(int minutes) {
        String sql = "SELECT toStartOfMinute(window_start) as t, " +
                     "       sum(pv_count) as pv " +
                     "FROM pv_minute " +
                     "WHERE window_start >= now() - INTERVAL ? MINUTE " +
                     "GROUP BY t ORDER BY t";
        return clickhouseJdbc.queryForList(sql, minutes);
    }

    /** 来源分析 */
    public List<Map<String, Object>> getRefererAnalysis() {
        String sql = "SELECT referer_type, sum(count) as cnt " +
                     "FROM referer_hourly " +
                     "WHERE window_start >= toDate(now()) " +
                     "GROUP BY referer_type";
        return clickhouseJdbc.queryForList(sql);
    }

    /** 漏斗分析 */
    public List<Map<String, Object>> getFunnelData() {
        String sql = "SELECT step_name, user_count, conversion_rate " +
                     "FROM funnel_analysis " +
                     "WHERE window_start >= toDate(now()) " +
                     "ORDER BY step_order";
        return clickhouseJdbc.queryForList(sql);
    }

    /** 平均响应时间（最近 5 分钟，缓存 10 秒） */
    public double getAvgResponseTime() {
        String cacheKey = "analytics:avgRt";
        String cached = redisTemplate.opsForValue().get(cacheKey);
        if (cached != null) {
            return Double.parseDouble(cached);
        }
        String sql = "SELECT avg(avg_response_time) FROM pv_minute " +
                     "WHERE window_start >= now() - INTERVAL 5 MINUTE";
        Double result = clickhouseJdbc.queryForObject(sql, Double.class);
        double rt = result != null ? result : 0.0;
        redisTemplate.opsForValue().set(cacheKey, String.valueOf(rt), CACHE_SHORT, TimeUnit.SECONDS);
        return rt;
    }
}
