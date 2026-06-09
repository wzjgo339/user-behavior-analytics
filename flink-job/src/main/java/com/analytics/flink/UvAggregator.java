package com.analytics.flink;

import com.analytics.flink.model.LogEvent;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.HashSet;

/**
 * UV 聚合：使用 HashSet 精确去重
 * 注意：千万级 UV 场景应替换为 HyperLogLog
 */
public class UvAggregator implements AggregateFunction<LogEvent, HashSet<String>, UvAggregator.UvResult> {

    public static class UvResult {
        public long windowStart;
        public long windowEnd;
        public long uvCount;
    }

    @Override
    public HashSet<String> createAccumulator() { return new HashSet<>(); }

    @Override
    public HashSet<String> add(LogEvent event, HashSet<String> acc) {
        acc.add(event.ip);
        return acc;
    }

    @Override
    public UvResult getResult(HashSet<String> acc) {
        UvResult r = new UvResult();
        r.uvCount = acc.size();
        return r;
    }

    @Override
    public HashSet<String> merge(HashSet<String> a, HashSet<String> b) {
        a.addAll(b);
        return a;
    }
}
