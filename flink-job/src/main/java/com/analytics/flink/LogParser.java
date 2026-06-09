package com.analytics.flink;

import com.analytics.flink.model.LogEvent;
import org.apache.flink.api.common.functions.MapFunction;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Locale;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 将原始日志行解析为 LogEvent 对象 */
public class LogParser implements MapFunction<String, LogEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(LogParser.class);
    private static final DateTimeFormatter FORMATTER =
            DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH);

    @Override
    public LogEvent map(String line) throws Exception {
        LogEvent e = new LogEvent();
        try {
            String[] parts = line.split("\\|", -1);
            if (parts.length < 8) return null;

            e.ip = parts[0];
            // parts[1] = remote_user（未使用）
            // parts[2] = [time_local]
            // 格式包含时区（如 +0000），需先解析为 ZonedDateTime 再转 LocalDateTime
            String timeStr = parts[2].replace("[", "").replace("]", "").trim();
            try {
                e.timestamp = ZonedDateTime.parse(timeStr, FORMATTER).toLocalDateTime();
            } catch (DateTimeParseException dtpe) {
                LOG.warn("Timestamp parse failed: '{}'", timeStr);
                return null;
            }

            // parts[3] = "GET /xxx HTTP/1.1"
            String request = parts[3].replaceAll("^\"|\"$", "");
            String[] reqParts = request.split(" ");
            e.method = reqParts.length > 0 ? reqParts[0] : "";
            e.url = reqParts.length > 1 ? reqParts[1] : "";

            e.status = Integer.parseInt(parts[4]);
            e.bodyBytes = Long.parseLong(parts[5]);

            // parts[6] = referer
            String ref = parts[6].replaceAll("^\"|\"$", "");
            e.referer = ref;
            e.refererType = classifyReferer(ref, e.url);

            e.userAgent = parts[7].replaceAll("^\"|\"$", "");

            if (parts.length > 8 && !parts[8].isEmpty()) {
                e.responseTime = Double.parseDouble(parts[8]);
            }
        } catch (Exception ex) {
            // 解析失败跳过该行
            return null;
        }
        return e;
    }

    private String classifyReferer(String referer, String url) {
        if (referer == null || referer.isEmpty() || referer.equals("-")) {
            return "direct";
        }
        String lower = referer.toLowerCase();
        if (lower.contains("google") || lower.contains("baidu") ||
            lower.contains("bing") || lower.contains("sogou")) {
            return "search_engine";
        }
        // 站内跳转（域名部分匹配 url 路径）
        if (lower.contains("localhost") || lower.contains("127.0.0.1")) {
            return "internal";
        }
        return "external";
    }
}
