# Phase 3：Flink 实时计算

## 目标

编写 Flink 作业，消费 Kafka 中的 Nginx 日志，实时计算 PV/UV/来源/状态码指标，写入 ClickHouse。

---

## 步骤 3.1：创建 Flink Maven 项目

```bash
mkdir -p flink-job/src/main/java/com/analytics/flink
mkdir -p flink-job/src/main/resources
```

`flink-job/pom.xml`：

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.analytics</groupId>
    <artifactId>flink-job</artifactId>
    <version>1.0.0</version>
    <packaging>jar</packaging>

    <properties>
        <java.version>17</java.version>
        <flink.version>1.19.0</flink.version>
        <clickhouse-jdbc.version>0.5.0</clickhouse-jdbc.version>
        <maven.compiler.source>17</maven.compiler.source>
        <maven.compiler.target>17</maven.compiler.target>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    </properties>

    <dependencies>
        <!-- Flink 核心 -->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-streaming-java</artifactId>
            <version>${flink.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-clients</artifactId>
            <version>${flink.version}</version>
        </dependency>
        <!-- Flink Kafka 连接器 -->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-connector-kafka</artifactId>
            <version>${flink.version}</version>
        </dependency>
        <!-- Flink ClickHouse JDBC -->
        <dependency>
            <groupId>com.clickhouse</groupId>
            <artifactId>clickhouse-jdbc</artifactId>
            <version>${clickhouse-jdbc.version}</version>
        </dependency>
        <!-- Flink JDBC 连接器 -->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-connector-jdbc</artifactId>
            <version>${flink.version}</version>
        </dependency>
        <!-- JSON 解析 -->
        <dependency>
            <groupId>com.fasterxml.jackson.core</groupId>
            <artifactId>jackson-databind</artifactId>
            <version>2.17.0</version>
        </dependency>
        <!-- 日志 -->
        <dependency>
            <groupId>ch.qos.logback</groupId>
            <artifactId>logback-classic</artifactId>
            <version>1.4.14</version>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-shade-plugin</artifactId>
                <version>3.5.1</version>
                <executions>
                    <execution>
                        <phase>package</phase>
                        <goals><goal>shade</goal></goals>
                        <configuration>
                            <transformers>
                                <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                                    <mainClass>com.analytics.flink.LogAnalysisJob</mainClass>
                                </transformer>
                            </transformers>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</project>
```

---

## 步骤 3.2：创建日志行解析类

`flink-job/src/main/java/com/analytics/flink/model/LogEvent.java`：

```java
package com.analytics.flink.model;

import java.time.LocalDateTime;

/** 解析后的单条日志记录 */
public class LogEvent {
    public String ip;
    public LocalDateTime timestamp;
    public String method;      // GET/POST
    public String url;
    public int status;
    public long bodyBytes;
    public String referer;
    public String userAgent;
    public double responseTime;  // 单位：秒
    public String refererType;   // direct / search_engine / external / internal

    public LogEvent() {}

    // full-args constructor, getters, setters 由 IDE 自动生成
    // 这里用公共字段简化，正式项目可以改为 private + getter/setter
}
```

`flink-job/src/main/java/com/analytics/flink/LogParser.java`：

```java
package com.analytics.flink;

import com.analytics.flink.model.LogEvent;
import org.apache.flink.api.common.functions.MapFunction;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

/** 将原始日志行解析为 LogEvent 对象 */
public class LogParser implements MapFunction<String, LogEvent> {

    private static final DateTimeFormatter FORMATTER =
            DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH);

    @Override
    public LogEvent map(String line) throws Exception {
        LogEvent e = new LogEvent();
        try {
            String[] parts = line.split("\\|", -1);
            if (parts.length < 8) return null;

            e.ip = parts[0];
            // parts[1] = remote_user（未使用）
            // parts[2] = [time_local]
            String timeStr = parts[2].replace("[", "").replace("]", "");
            e.timestamp = LocalDateTime.parse(timeStr, FORMATTER);

            // parts[3] = "GET /xxx HTTP/1.1"
            String request = parts[3].replaceAll("^\"|\"$", "");
            String[] reqParts = request.split(" ");
            e.method = reqParts.length > 0 ? reqParts[0] : "";
            e.url = reqParts.length > 1 ? reqParts[1] : "";

            e.status = Integer.parseInt(parts[4]);
            e.bodyBytes = Long.parseLong(parts[5]);

            // parts[6] = referer
            String ref = parts[6].replaceAll("^\"|\"$", "");
            e.referer = ref;
            e.refererType = classifyReferer(ref, e.url);

            e.userAgent = parts[7].replaceAll("^\"|\"$", "");

            if (parts.length > 8 && !parts[8].isEmpty()) {
                e.responseTime = Double.parseDouble(parts[8]);
            }
        } catch (Exception ex) {
            // 解析失败跳过该行
            return null;
        }
        return e;
    }

    private String classifyReferer(String referer, String url) {
        if (referer == null || referer.isEmpty() || referer.equals("-")) {
            return "direct";
        }
        String lower = referer.toLowerCase();
        if (lower.contains("google") || lower.contains("baidu") ||
            lower.contains("bing") || lower.contains("sogou")) {
            return "search_engine";
        }
        // 站内跳转（域名部分匹配 url 路径）
        if (lower.contains("localhost") || lower.contains("127.0.0.1")) {
            return "internal";
        }
        return "external";
    }
}
```

---

## 步骤 3.3：实现 Flink 主作业

`flink-job/src/main/java/com/analytics/flink/LogAnalysisJob.java`：

```java
package com.analytics.flink;

import com.analytics.flink.model.LogEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class LogAnalysisJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // ===== 1. 读取 Kafka 数据源 =====
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("kafka:9092")
                .setTopics("nginx-access-log")
                .setGroupId("flink-analytics")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new org.apache.flink.api.common.serialization.SimpleStringSchema())
                .build();

        DataStream<LogEvent> logStream = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source")
                .map(new LogParser())
                .filter(e -> e != null);  // 过滤解析失败的行

        // ===== 2. 计算 PV（每分钟按 URL 分组计数）=====
        logStream
                .keyBy((KeySelector<LogEvent, String>) e -> e.url)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .aggregate(new PvAggregator())
                .map(new ClickHouseWriter.PvMinuteWriter())
                .name("pv-minute-sink");

        // ===== 3. 计算 UV（每分钟 HyperLogLog 近似去重）=====
        logStream
                .windowAll(TumblingEventTimeWindows.of(Time.minutes(1)))
                .aggregate(new UvAggregator())
                .map(new ClickHouseWriter.UvMinuteWriter())
                .name("uv-minute-sink");

        // ===== 4. 来源分析（每小时按来源类型分组）=====
        logStream
                .keyBy((KeySelector<LogEvent, String>) e -> e.refererType)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new RefererAggregator())
                .map(new ClickHouseWriter.RefererHourlyWriter())
                .name("referer-sink");

        // ===== 5. 状态码分布（每分钟）=====
        logStream
                .keyBy((KeySelector<LogEvent, Integer>) e -> e.status / 100 * 100)  // 按 2xx,3xx,4xx,5xx 分组
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .aggregate(new StatusAggregator())
                .map(new ClickHouseWriter.StatusMinuteWriter())
                .name("status-sink");

        // ===== 6. 写入原始日志采样（仅保留 7 天）=====
        logStream
                .map(new ClickHouseWriter.LogSampleWriter())
                .name("log-sample-sink");

        env.execute("nginx-log-analysis");
    }
}
```

---

## 步骤 3.4：实现聚合函数

### PV 聚合

`flink-job/src/main/java/com/analytics/flink/PvAggregator.java`：

```java
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
```

### UV 聚合

`flink-job/src/main/java/com/analytics/flink/UvAggregator.java`：

```java
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
```

### 来源聚合

`flink-job/src/main/java/com/analytics/flink/RefererAggregator.java`：结构同 PvAggregator，按 `refererType` 分组计数。

### 状态码聚合

`flink-job/src/main/java/com/analytics/flink/StatusAggregator.java`：按状态码分组计数。

---

## 步骤 3.5：实现 ClickHouse 写入

`flink-job/src/main/java/com/analytics/flink/ClickHouseWriter.java`：

```java
package com.analytics.flink;

import com.analytics.flink.model.LogEvent;
import org.apache.flink.api.common.functions.MapFunction;
import java.sql.*;
import java.time.Instant;

/**
 * ClickHouse 写入器
 * 每个 Writer 类执行 INSERT 操作
 * 实际生产环境可用 JDBCSink 或异步批量写入
 */
public class ClickHouseWriter {

    private static final String JDBC_URL = "jdbc:clickhouse://clickhouse:8123/default";
    private static final String DRIVER = "com.clickhouse.jdbc.ClickHouseDriver";

    static {
        try {
            Class.forName(DRIVER);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("ClickHouse JDBC Driver not found", e);
        }
    }

    private static Connection getConn() throws SQLException {
        return DriverManager.getConnection(JDBC_URL);
    }

    // === PV 分钟写入 ===
    public static class PvMinuteWriter implements MapFunction<PvAggregator.PvResult, String> {
        @Override
        public String map(PvAggregator.PvResult r) throws Exception {
            try (Connection conn = getConn();
                 PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO pv_minute (window_start, window_end, url, pv_count, " +
                    "status_2xx, status_3xx, status_4xx, status_5xx, avg_response_time) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
                ps.setLong(1, r.windowStart);
                ps.setLong(2, r.windowEnd);
                ps.setString(3, r.url);
                ps.setLong(4, r.count);
                ps.setLong(5, r.status2xx);
                ps.setLong(6, r.status3xx);
                ps.setLong(7, r.status4xx);
                ps.setLong(8, r.status5xx);
                ps.setDouble(9, r.avgResponseTime);
                ps.executeUpdate();
            }
            return "ok";
        }
    }

    // === UV 分钟写入 ===
    public static class UvMinuteWriter implements MapFunction<UvAggregator.UvResult, String> {
        @Override
        public String map(UvAggregator.UvResult r) throws Exception {
            try (Connection conn = getConn();
                 PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO uv_minute (window_start, window_end, uv_count) VALUES (?, ?, ?)")) {
                ps.setLong(1, r.windowStart);
                ps.setLong(2, r.windowEnd);
                ps.setLong(3, r.uvCount);
                ps.executeUpdate();
            }
            return "ok";
        }
    }

    // === 来源写入（略，类似结构）===

    // === 采样日志写入 ===
    public static class LogSampleWriter implements MapFunction<LogEvent, String> {
        @Override
        public String map(LogEvent e) throws Exception {
            try (Connection conn = getConn();
                 PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO access_log_sample (timestamp, ip, url, status, " +
                    "response_time, referer, user_agent) VALUES (?, ?, ?, ?, ?, ?, ?)")) {
                // ... 字段映射
                ps.executeUpdate();
            }
            return "ok";
        }
    }
}
```

> 注意：上面的每个 Writer 都打开一个独立连接写入 ClickHouse，测试环境可用。
> 生产环境需要批量写入（如每 10 秒或每 1000 条 flush 一次）。

---

## 步骤 3.6：编译并运行 Flink 作业

### 编译

```bash
cd flink-job
mvn clean package -DskipTests
```

### 提交到 Flink（Docker 方式）

```bash
# 先确保有 Flink 容器（可单独启动）
docker run -d --name flink-jobmanager \
  --network user-behavior-analytics_analytics-net \
  -e JOB_MANAGER_RPC_ADDRESS=flink-jobmanager \
  flink:1.19 scala=1

docker run -d --name flink-taskmanager \
  --network user-behavior-analytics_analytics-net \
  -e JOB_MANAGER_RPC_ADDRESS=flink-jobmanager \
  flink:1.19 taskmanager

# 提交作业
docker cp flink-job/target/flink-job-1.0.0.jar flink-jobmanager:/job.jar
docker exec flink-jobmanager flink run -d /job.jar
```

### 验证作业运行

```bash
docker exec flink-jobmanager flink list
```

如果显示作业 `RUNNING`，同时 ClickHouse 中开始有数据写入，即成功。

---

## 备选方案（如果 Flink 部署遇到困难）

如果本地 Docker 跑 Flink 集群太复杂，可以用替代方式消费 Kafka 并写入 ClickHouse：

### 方式 A：Kafka Streams（Java 库，无额外部署）
- 在 Spring Boot 中引入 `kafka-streams` 依赖
- 在 Phase 4 的 `server` 模块中直接实现流处理逻辑
- 优点：不需要单独部署 Flink 集群
- 缺点：窗口语义不如 Flink 丰富

### 方式 B：Python 脚本
```python
from kafka import KafkaConsumer
from clickhouse_driver import Client
import json

client = Client(host='localhost')
consumer = KafkaConsumer('nginx-access-log', bootstrap_servers='localhost:9092')

for msg in consumer:
    line = msg.value.decode()
    # 解析并写入 ClickHouse
    client.execute('INSERT INTO access_log_sample VALUES', [parsed_data])
```

---

## 本阶段完成标志

- [ ] Flink 作业成功编译（`mvn package` 无报错）
- [ ] Flink 作业已提交并显示 `RUNNING`
- [ ] 日志模拟器运行时，ClickHouse `pv_minute` 表有数据写入
- [ ] `uv_minute` 表写入正常（UV 计数合理）
- [ ] 通过 Kill 模拟器再重启，确认 Flink 能从 Kafka 最新偏移量继续消费
