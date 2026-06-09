package com.analytics.server.service;

import com.analytics.server.model.RealtimeData;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/**
 * WebSocket 实时数据推送服务
 * 每 5 秒从 ClickHouse 拉取聚合数据并推送到前端
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class RealtimePushService {

    private final SimpMessagingTemplate messagingTemplate;
    private final AnalyticsService analyticsService;

    @Scheduled(fixedRateString = "${analytics.push-interval:5000}")
    public void pushRealtimeData() {
        try {
            RealtimeData data = new RealtimeData(
                System.currentTimeMillis(),
                analyticsService.getTodayPv(),
                analyticsService.getTodayUv(),
                analyticsService.getTopPages(10),
                analyticsService.getStatusDistribution(),
                analyticsService.getAvgResponseTime()
            );
            messagingTemplate.convertAndSend("/topic/realtime", data);
        } catch (Exception e) {
            log.error("推送实时数据失败", e);
        }
    }
}
