package com.analytics.flink;

import com.analytics.flink.model.LogEvent;
import org.apache.flink.api.common.functions.AggregateFunction;

/** PV 聚合：按 URL 分组，同时统计各状态码数量和平均响应时间 */
public class PvAggregator implements AggregateFunction<LogEvent, PvAggregator.PvAccumulator, PvAggregator.PvResult> {

    public static class PvAccumulator {
        long count;
        long count2xx, count3xx, count4xx, count5xx;
        double totalResponseTime;
    }

    public static class PvResult {
        public long windowStart;
        public long windowEnd;
        public String url;
        public long count;
        public long status2xx, status3xx, status4xx, status5xx;
        public double avgResponseTime;
    }

    @Override
    public PvAccumulator createAccumulator() { return new PvAccumulator(); }

    @Override
    public PvAccumulator add(LogEvent event, PvAccumulator acc) {
        acc.count++;
        int code = event.status;
        if (code >= 200 && code < 300) acc.count2xx++;
        else if (code >= 300 && code < 400) acc.count3xx++;
        else if (code >= 400 && code < 500) acc.count4xx++;
        else if (code >= 500) acc.count5xx++;
        acc.totalResponseTime += event.responseTime;
        return acc;
    }

    @Override
    public PvResult getResult(PvAccumulator acc) {
        PvResult r = new PvResult();
        r.count = acc.count;
        r.status2xx = acc.count2xx;
        r.status3xx = acc.count3xx;
        r.status4xx = acc.count4xx;
        r.status5xx = acc.count5xx;
        r.avgResponseTime = acc.count > 0 ? acc.totalResponseTime / acc.count : 0;
        return r;
    }

    @Override
    public PvAccumulator merge(PvAccumulator a, PvAccumulator b) {
        a.count += b.count;
        a.count2xx += b.count2xx;
        a.count3xx += b.count3xx;
        a.count4xx += b.count4xx;
        a.count5xx += b.count5xx;
        a.totalResponseTime += b.totalResponseTime;
        return a;
    }
}
