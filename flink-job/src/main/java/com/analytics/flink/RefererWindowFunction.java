package com.analytics.flink;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/** 来源分析窗口函数：补全 windowStart/windowEnd/refererType */
public class RefererWindowFunction
        extends ProcessWindowFunction<RefererAggregator.RefererResult, RefererAggregator.RefererResult, String, TimeWindow> {

    @Override
    public void process(String key,
                        Context context,
                        Iterable<RefererAggregator.RefererResult> elements,
                        Collector<RefererAggregator.RefererResult> out) {
        RefererAggregator.RefererResult r = elements.iterator().next();
        r.windowStart = context.window().getStart();
        r.windowEnd = context.window().getEnd();
        r.refererType = key;
        out.collect(r);
    }
}
