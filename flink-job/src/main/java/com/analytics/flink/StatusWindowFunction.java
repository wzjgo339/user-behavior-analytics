package com.analytics.flink;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/** 状态码分布窗口函数：补全 windowStart/statusCode */
public class StatusWindowFunction
        extends ProcessWindowFunction<StatusAggregator.StatusResult, StatusAggregator.StatusResult, Integer, TimeWindow> {

    @Override
    public void process(Integer key,
                        Context context,
                        Iterable<StatusAggregator.StatusResult> elements,
                        Collector<StatusAggregator.StatusResult> out) {
        StatusAggregator.StatusResult r = elements.iterator().next();
        r.windowStart = context.window().getStart();
        r.statusCode = key;
        out.collect(r);
    }
}
