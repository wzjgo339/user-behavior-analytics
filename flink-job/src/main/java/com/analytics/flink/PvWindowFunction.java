package com.analytics.flink;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * PV 窗口函数：从 Window 上下文中提取 windowStart/windowEnd，
 * 补全 PvAggregator 聚合结果中的时间字段。
 */
public class PvWindowFunction
        extends ProcessWindowFunction<PvAggregator.PvResult, PvAggregator.PvResult, String, TimeWindow> {

    @Override
    public void process(String key,
                        Context context,
                        Iterable<PvAggregator.PvResult> elements,
                        Collector<PvAggregator.PvResult> out) {
        PvAggregator.PvResult r = elements.iterator().next();
        r.windowStart = context.window().getStart();
        r.windowEnd = context.window().getEnd();
        r.url = key;
        out.collect(r);
    }
}
