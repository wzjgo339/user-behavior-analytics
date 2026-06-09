package com.analytics.flink.model;

import java.time.LocalDateTime;

/** 解析后的单条日志记录 */
public class LogEvent {
    public String ip;
    public LocalDateTime timestamp;
    public String method;      // GET/POST
    public String url;
    public int status;
    public long bodyBytes;
    public String referer;
    public String userAgent;
    public double responseTime;  // 单位：秒
    public String refererType;   // direct / search_engine / external / internal

    public LogEvent() {}
}
