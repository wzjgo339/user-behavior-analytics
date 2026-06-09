package com.analytics.flink;

import com.analytics.flink.model.LogEvent;
import org.apache.flink.api.common.functions.AggregateFunction;

/** 来源分析聚合：按 refererType 分组计数 */
public class RefererAggregator implements AggregateFunction<LogEvent, RefererAggregator.RefererAccumulator, RefererAggregator.RefererResult> {

    public static class RefererAccumulator {
        long count;
    }

    public static class RefererResult {
        public long windowStart;
        public long windowEnd;
        public String refererType;
        public long count;
    }

    @Override
    public RefererAccumulator createAccumulator() { return new RefererAccumulator(); }

    @Override
    public RefererAccumulator add(LogEvent event, RefererAccumulator acc) {
        acc.count++;
        return acc;
    }

    @Override
    public RefererResult getResult(RefererAccumulator acc) {
        RefererResult r = new RefererResult();
        r.count = acc.count;
        return r;
    }

    @Override
    public RefererAccumulator merge(RefererAccumulator a, RefererAccumulator b) {
        a.count += b.count;
        return a;
    }
}
