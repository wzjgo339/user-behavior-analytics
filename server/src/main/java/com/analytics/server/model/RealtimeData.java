package com.analytics.server.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import java.util.List;
import java.util.Map;

/** WebSocket 实时推送的数据结构 */
@Data
@AllArgsConstructor
public class RealtimeData {
    private long timestamp;
    private long pv;
    private long uv;
    private List<Map<String, Object>> topPages;     // [{url, pv}]
    private Map<String, Long> statusCodes;           // {"2xx": N, "3xx": N, ...}
    private double avgResponseTime;
}
