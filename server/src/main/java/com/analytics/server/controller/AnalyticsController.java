package com.analytics.server.controller;

import com.analytics.server.service.AnalyticsService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * REST API 控制器
 * 提供 ClickHouse 分析数据的 HTTP 接口
 */
@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class AnalyticsController {

    private final AnalyticsService service;

    /** 概览数据 */
    @GetMapping("/overview")
    public Map<String, Object> overview() {
        return Map.of(
            "pv", service.getTodayPv(),
            "uv", service.getTodayUv(),
            "topPages", service.getTopPages(10),
            "statusCodes", service.getStatusDistribution(),
            "avgResponseTime", service.getAvgResponseTime()
        );
    }

    /** PV 趋势 */
    @GetMapping("/pv/trend")
    public List<Map<String, Object>> pvTrend(
            @RequestParam(defaultValue = "60") int minutes) {
        return service.getPvTrend(minutes);
    }

    /** TOP 页面 */
    @GetMapping("/pv/top")
    public List<Map<String, Object>> topPages(
            @RequestParam(defaultValue = "10") int limit) {
        return service.getTopPages(limit);
    }

    /** 来源分析 */
    @GetMapping("/referer")
    public List<Map<String, Object>> referer() {
        return service.getRefererAnalysis();
    }

    /** 漏斗数据 */
    @GetMapping("/funnel")
    public List<Map<String, Object>> funnel() {
        return service.getFunnelData();
    }

    /** 健康检查 */
    @GetMapping("/health")
    public String health() { return "ok"; }
}
