package com.analytics.flink;

import com.analytics.flink.model.LogEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.time.ZoneOffset;

/**
 * Flink 主作业：消费 Kafka Nginx 日志，实时计算指标并写入 ClickHouse。
 *
 * 修正说明：
 * 文档中 PvAggregator/UvAggregator 的 windowStart/windowEnd 字段在 AggregateFunction
 * 中无法获取窗口上下文，因此额外增加了对应的 ProcessWindowFunction 来补全时间字段。
 */
public class LogAnalysisJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 开启 Checkpoint（默认每隔 10s，保证 Exactly-Once 语义）
        env.enableCheckpointing(10_000);
        env.setParallelism(1);

        // ===== 1. 读取 Kafka 数据源 =====
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("kafka:9092")
                .setTopics("nginx-access-log")
                .setGroupId("flink-analytics")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(
                        new org.apache.flink.api.common.serialization.SimpleStringSchema())
                .build();

        DataStream<LogEvent> logStream = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source")
                // Filebeat 发到 Kafka 的是 JSON，需要提取 message 字段
                .map(new FilebeatMessageExtractor())
                .map(new LogParser())
                // 过滤解析失败的行（LogParser 返回 null）
                .filter(e -> e != null)
                // 提取 Event Time + 允许 5 秒乱序
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<LogEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, timestamp) ->
                                        event.timestamp.toInstant(ZoneOffset.UTC).toEpochMilli()
                                )
                );

        // ===== 2. 计算 PV（每分钟按 URL 分组计数）=====
        logStream
                .keyBy((KeySelector<LogEvent, String>) e -> e.url)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .aggregate(new PvAggregator(), new PvWindowFunction())
                .map(new ClickHouseWriter.PvMinuteWriter())
                .name("pv-minute-sink");

        // ===== 3. 计算 UV（每分钟全窗口精确去重）=====
        logStream
                .windowAll(TumblingEventTimeWindows.of(Time.minutes(1)))
                .aggregate(new UvAggregator(), new UvWindowFunction())
                .map(new ClickHouseWriter.UvMinuteWriter())
                .name("uv-minute-sink");

        // ===== 4. 来源分析（每小时按来源类型分组）=====
        logStream
                .keyBy((KeySelector<LogEvent, String>) e -> e.refererType)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new RefererAggregator(), new RefererWindowFunction())
                .map(new ClickHouseWriter.RefererHourlyWriter())
                .name("referer-sink");

        // ===== 5. 状态码分布（每分钟按 2xx/3xx/4xx/5xx 分组）=====
        logStream
                .keyBy((KeySelector<LogEvent, Integer>) e -> e.status / 100 * 100)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .aggregate(new StatusAggregator(), new StatusWindowFunction())
                .map(new ClickHouseWriter.StatusMinuteWriter())
                .name("status-sink");

        // ===== 6. 写入原始日志采样（每条记录逐条写入，生产环境应批量）=====
        logStream
                .map(new ClickHouseWriter.LogSampleWriter())
                .name("log-sample-sink");

        env.execute("nginx-log-analysis");
    }
}
