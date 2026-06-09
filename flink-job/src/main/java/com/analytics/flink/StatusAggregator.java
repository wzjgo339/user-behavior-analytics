package com.analytics.flink;

import com.analytics.flink.model.LogEvent;
import org.apache.flink.api.common.functions.AggregateFunction;

/** 状态码分布聚合：按状态分类（2xx/3xx/4xx/5xx）计数 */
public class StatusAggregator implements AggregateFunction<LogEvent, StatusAggregator.StatusAccumulator, StatusAggregator.StatusResult> {

    public static class StatusAccumulator {
        long count;
    }

    public static class StatusResult {
        public long windowStart;
        public int statusCode;  // 状态分类：200, 300, 400, 500
        public long count;
    }

    @Override
    public StatusAccumulator createAccumulator() { return new StatusAccumulator(); }

    @Override
    public StatusAccumulator add(LogEvent event, StatusAccumulator acc) {
        acc.count++;
        return acc;
    }

    @Override
    public StatusResult getResult(StatusAccumulator acc) {
        StatusResult r = new StatusResult();
        r.count = acc.count;
        return r;
    }

    @Override
    public StatusAccumulator merge(StatusAccumulator a, StatusAccumulator b) {
        a.count += b.count;
        return a;
    }
}
