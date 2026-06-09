package com.analytics.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.Map;

/**
 * Filebeat JSON 消息提取器
 * Filebeat 发送到 Kafka 的是 JSON 格式，原始日志行在 "message" 字段中。
 * 如果消息不是 JSON（如直接发送原始日志的调试场景），则原样返回。
 */
public class FilebeatMessageExtractor implements MapFunction<String, String> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public String map(String value) throws Exception {
        // 快速判断：不以 { 开头说明不是 JSON，直接返回
        if (value == null || value.isEmpty() || value.charAt(0) != '{') {
            return value;
        }

        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> parsed = MAPPER.readValue(value, Map.class);
            Object msg = parsed.get("message");
            if (msg != null && msg instanceof String) {
                return (String) msg;
            }
        } catch (Exception ignored) {
            // 解析失败按原始值处理
        }
        return value;
    }
}
