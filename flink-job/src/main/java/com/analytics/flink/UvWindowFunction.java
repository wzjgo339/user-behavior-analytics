package com.analytics.flink;

import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/** UV 窗口函数：补全 windowStart/windowEnd 时间字段
 *  （用于 windowAll 场景，因此继承 ProcessAllWindowFunction） */
public class UvWindowFunction
        extends ProcessAllWindowFunction<UvAggregator.UvResult, UvAggregator.UvResult, TimeWindow> {

    @Override
    public void process(Context context,
                        Iterable<UvAggregator.UvResult> elements,
                        Collector<UvAggregator.UvResult> out) {
        UvAggregator.UvResult r = elements.iterator().next();
        r.windowStart = context.window().getStart();
        r.windowEnd = context.window().getEnd();
        out.collect(r);
    }
}
