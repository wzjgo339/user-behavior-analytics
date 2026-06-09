# Phase 3 踩坑与解决

## 目录

1. [Maven 依赖版本不匹配](#1-maven-依赖版本不匹配)
2. [Java 版本不兼容（Flink 镜像 JDK 11 vs JAR JDK 17）](#2-java-版本不兼容flink-镜像-jdk-11-vs-jar-jdk-17)
3. [ClickHouse JDBC 驱动与 26.x 服务器协议不兼容](#3-clickhouse-jdbc-驱动与-26x-服务器协议不兼容)
4. [ClickHouse default 用户仅限 localhost 访问](#4-clickhouse-default-用户仅限-localhost-访问)
5. [ClickHouse JDBC 0.9.8 拒绝空密码参数](#5-clickhouse-jdbc-098-拒绝空密码参数)
6. [Filebeat 发送 JSON 而非纯文本到 Kafka](#6-filebeat-发送-json-而非纯文本到-kafka)
7. [LocalDateTime 无法解析带时区的时间戳格式](#7-localdatetime-无法解析带时区的时间戳格式)
8. [Python datetime.now() 生成 naive datetime 导致 %z 输出空白](#8-python-datetimenow-生成-naive-datetime-导致-z-输出空白)
9. [TumblingEventTimeWindows 需要显式分配 Event Time](#9-tumblingeventtimewindows-需要显式分配-event-time)
10. [ProcessWindowFunction 缺失 — AggregateFunction 取不到窗口上下文](#10-processwindowfunction-缺失--aggregatefunction-取不到窗口上下文)

---

## 1. Maven 依赖版本不匹配

### 症状

```
[ERROR] Failed to execute goal on project flink-job:
Could not resolve dependencies:
org.apache.flink:flink-connector-kafka:jar:1.19.0 was not found
```

### 原因

Flink 从 1.17 开始将连接器（connector）从核心 Flink 发布周期中剥离，使用独立的版本号。`flink-connector-kafka:1.19.0` 这个 artifact 不存在于任何 Maven 仓库。同时 `flink-connector-jdbc` 也被剥离。

### 解决

查阅 Maven 仓库元数据，找到与 Flink 1.19 兼容的 connector 版本：

| 依赖 | 正确版本 |
|---|---|
| `flink-connector-kafka` | `3.3.0-1.19` |
| `flink-connector-jdbc` | `3.2.0-1.19`（实际未使用，已从 pom.xml 移除） |

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-connector-kafka</artifactId>
    <version>3.3.0-1.19</version>
</dependency>
```

验证方式：查询 Maven 仓库元数据：
```bash
curl -s "https://maven.aliyun.com/repository/public/org/apache/flink/flink-connector-kafka/maven-metadata.xml"
```

---

## 2. Java 版本不兼容（Flink 镜像 JDK 11 vs JAR JDK 17）

### 症状

```
Caused by: java.lang.UnsupportedClassVersionError:
com/analytics/flink/LogAnalysisJob has been compiled by a more recent
version of the Java Runtime (class file version 61.0),
this version of the Java Runtime only recognizes class file versions up to 55.0
```

### 原因

Flink 1.19 Docker 镜像（`flink:1.19`）基于 **Java 11**（class version 55），而我们的 Maven 项目默认使用 `--release 17`（class version 61）编译。JAR 中的 class 文件版本高于 Flink 容器中的 JVM 版本。

### 解决

在 `pom.xml` 中设置 `<release>` 为 11：

```xml
<properties>
    <maven.compiler.release>11</maven.compiler.release>
</properties>
```

如果使用 `<maven.compiler.source>/<maven.compiler.target>` 或单独设置 source/target，这些方式可能在 Maven Compiler Plugin 3.15+ 中仍使用 `--release` 标记错误的 JDK 版本。正确的显式配置方式：

```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-compiler-plugin</artifactId>
    <configuration>
        <release>11</release>
    </configuration>
</plugin>
```

验证编译后的 class 版本：
```bash
# class version 55 = Java 11
javap -verbose LogAnalysisJob.class | grep "major version"
# 或读取 class 文件字节 7-8:
od -A n -t d1 LogAnalysisJob.class | head -1 | awk '{print $2}'
```

---

## 3. ClickHouse JDBC 驱动与 26.x 服务器协议不兼容

### 症状

```
Caused by: java.sql.SQLException:
java.io.IOException: Magic is not correct - expect [-126] but got [-107]
```

Flink 作业反复重启（`RESTARTING` 状态），taskmanager 日志中出现 LZ4 magic 错误。

### 原因

`clickhouse-jdbc:0.5.0`（2022 年发布）使用旧版的二进制协议和 LZ4 压缩格式，与 ClickHouse 26.5.1（2026 年发布）的通信协议不兼容。服务器返回的数据头中的 LZ4 magic 字节与旧驱动期待的不匹配。

### 解决

升级 `clickhouse-jdbc` 到 `0.9.8`：

```xml
<clickhouse-jdbc.version>0.9.8</clickhouse-jdbc.version>
```

```xml
<dependency>
    <groupId>com.clickhouse</groupId>
    <artifactId>clickhouse-jdbc</artifactId>
    <version>${clickhouse-jdbc.version}</version>
</dependency>
```

Maven 仓库中 0.5.0 → 0.9.8 之间的版本变化：
- 0.6.x 系列：重构了 ClickHouse HTTP 客户端
- 0.7.x 系列：改进了数据类型映射
- 0.8.x 系列：增强对 ClickHouse 22.x+ 的支持
- 0.9.x 系列：全面兼容 ClickHouse 24.x-26.x

额外影响：升级后需要 Apache HTTP Client 5 类库（`org.apache.hc.core5.http.ClassicHttpRequest`），若类路径中不存在则回退到 `HTTP_URL_CONNECTION`，此警告不影响功能。

---

## 4. ClickHouse default 用户仅限 localhost 访问

### 症状

Flink 写入数据时，ClickHouse 返回：

```
Code: 194. DB::Exception: default: Authentication failed: password is incorrect,
or there is no user with such name
```

但 `clickhouse-client` 在容器内直接执行可正常登录。

### 原因

ClickHouse 26.x 的 Docker 镜像在 `/etc/clickhouse-server/users.d/default-password.xml` 中自动生成了安全配置：

```xml
<default>
    <networks>
        <ip>::1</ip>
        <ip>127.0.0.1</ip>
    </networks>
</default>
```

`default` 用户被限制为仅能从本地（localhost/IPv6 localhost）连接，远程连接（包括同一 Docker 网络中的其他容器）被拒绝。

> 注意：`users.d/` 目录中的配置会**覆盖** `users.xml` 中的对应项。即使 `users.xml` 中设置了 `<ip>::/0</ip>`（允许所有地址），`users.d/default-password.xml` 中的限制仍会生效。

### 解决

**方案：** 创建专用的 `analytics` 用户（无密码身份验证）：

```sql
CREATE USER IF NOT EXISTS analytics IDENTIFIED WITH no_password;
GRANT SELECT, INSERT, CREATE, ALTER, DROP, TRUNCATE ON default.* TO analytics;
```

同时将 `ClickHouseWriter.java` 中的 JDBC URL 改为以 `analytics` 用户连接。

该用户通过 ClickHouse 的 init SQL 脚本（`docker-entrypoint-initdb.d/01-create-tables.sql`）创建，容器首次启动时自动执行：

```sql
CREATE USER IF NOT EXISTS analytics IDENTIFIED WITH no_password;
GRANT ALL ON *.* TO analytics;
```

> 注意：如果在容器已运行后添加该 SQL，需要手动执行 `CREATE USER` 命令或重启容器。

---

## 5. ClickHouse JDBC 0.9.8 拒绝空密码参数

### 症状

```
Caused by: java.sql.SQLException:
Invalid query parameter value in pair 'password='
```

### 原因

`clickhouse-jdbc:0.9.8` 在解析 JDBC URL 参数时进行更严格的校验。URL 中的 `password=`（没有值的空参数）被判定为无效。

```java
// 错误写法（0.9.8 拒绝空值）
private static final String JDBC_URL =
    "jdbc:clickhouse://clickhouse:8123/default?user=analytics&password=";
```

### 解决

删除 `password` 参数，仅保留 `user`：

```java
private static final String JDBC_URL =
    "jdbc:clickhouse://clickhouse:8123/default?user=analytics";
```

同时，`DriverManager.getConnection(url, user, password)` 方式在旧版驱动（0.5.0）中存在用户/密码不生效的问题，改为直接在 URL 中传参可兼容新旧版本。

```java
// 推荐：URL 中直接包含用户参数
private static Connection getConn() throws SQLException {
    return DriverManager.getConnection(JDBC_URL);
}
```

---

## 6. Filebeat 发送 JSON 而非纯文本到 Kafka

### 症状

Flink 作业 RUNNING，但所有表（包括 `access_log_sample`）持续 0 行写入。

通过 REST API 查看 Flink 算子指标：

```
Source.numRecordsIn = 326
Map.numRecordsIn = 326, Map.numRecordsOut = 326
Filter.numRecordsIn = 326, Filter.numRecordsOut = 0
```

所有 326 条记录被 `Filter` 过滤掉（`LogParser` 返回 null）。

### 原因

Filebeat 默认以 **JSON 格式** 发送数据到 Kafka 输出。每条 Kafka 消息的结构为：

```json
{
  "@timestamp": "2026-06-09T09:56:28.868Z",
  "message": "192.168.1.1|-|[09/Jun/2026:09:45:07 +0000]|\"GET /index HTTP/1.1\"|200|512|...",
  "fields": {"log_type": "nginx_access"},
  ...
}
```

Flink 使用 `SimpleStringSchema` 反序列化 Kafka 消息，得到的是整个 JSON 字符串，而不是原始日志行。`LogParser` 收到 JSON 字符串后尝试按 `|` 分割，自然无法匹配预期的管道符格式，全部返回 null。

### 解决

增加 `FilebeatMessageExtractor` 作为预处理步骤，放在 `LogParser` 之前：

```java
/**
 * Filebeat JSON 消息提取器
 * 如果消息是 JSON 格式，提取 "message" 字段中的原始日志行；
 * 如果不是 JSON，原样返回（适配调试场景）。
 */
public class FilebeatMessageExtractor implements MapFunction<String, String> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public String map(String value) throws Exception {
        if (value == null || value.isEmpty() || value.charAt(0) != '{') {
            return value;  // 不是 JSON，原样返回
        }
        try {
            Map<String, Object> parsed = MAPPER.readValue(value, Map.class);
            Object msg = parsed.get("message");
            if (msg instanceof String) {
                return (String) msg;
            }
        } catch (Exception ignored) {
            // JSON 解析失败也原样返回
        }
        return value;
    }
}
```

在数据流中插入提取步骤：

```java
DataStream<LogEvent> logStream = env
    .fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source")
    .map(new FilebeatMessageExtractor())   // <-- 新增：JSON → message 字段
    .map(new LogParser())                   // 原始日志行 → LogEvent
    .filter(e -> e != null);
```

### 验证方式

直接消费 Kafka 消息查看格式：
```bash
docker exec kafka kafka-console-consumer \
    --bootstrap-server localhost:19092 \
    --topic nginx-access-log --max-messages 1
```

---

## 7. LocalDateTime 无法解析带时区的时间戳格式

### 症状

`LogParser` 对**所有**消息返回 null，即使 `FilebeatMessageExtractor` 已正确提取了 message 字段。Flink 指标：`Filter.numRecordsOut = 0`。

taskmanager 日志：
```
WARN  LogParser - Timestamp parse failed: '09/Jun/2026:10:43:24 '
```

### 原因

时间戳格式 `09/Jun/2026:09:45:07 +0000` 中包含 **时区偏移**（`+0000`），但代码使用了 `LocalDateTime.parse()`：

```java
// DateTimeFormatter 包含时区模式 'Z'
private static final DateTimeFormatter FORMATTER =
    DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH);

// 错误：LocalDateTime 无法承载时区信息
e.timestamp = LocalDateTime.parse(timeStr, FORMATTER);
```

`DateTimeFormatter` 解析到 `Z` 模式（时区偏移）时，会构造一个 `ZonedDateTime` 或 `OffsetDateTime`，但 `LocalDateTime::from` 无法从这些类型中提取 `LocalDateTime`，抛出 `DateTimeParseException`。

### 解决

先解析为 `ZonedDateTime`，再转换为 `LocalDateTime`：

```java
import java.time.ZonedDateTime;

e.timestamp = ZonedDateTime.parse(timeStr, FORMATTER).toLocalDateTime();
```

同时添加 `.trim()` 处理时间戳字符串首尾可能的空白字符：

```java
String timeStr = parts[2].replace("[", "").replace("]", "").trim();
e.timestamp = ZonedDateTime.parse(timeStr, FORMATTER).toLocalDateTime();
```

### 时序分析（阶段性混淆）

调试过程中曾出现一个现象：调试日志显示 `timeStr='09/Jun/2026:10:43:24 '`（无时区、有末尾空格），与原 Kafka 消息中 `09/Jun/2026:09:45:07 +0000` 的格式矛盾。原因是 **此处有两个不同来源的时间戳**：

| 来源 | 格式 | 备注 |
|---|---|---|
| Docker 健康检查请求 | `09/Jun/2026:09:45:07 +0000` | Nginx 容器自身 |
| 模拟器生成的日志（修复前） | `09/Jun/2026:10:43:24 ` | Java `LocalDateTime` 解析失败 |
| 模拟器生成的日志（修复后） | `09/Jun/2026:10:45:19 +0000` | 见下节 |

初始 Kafka 中的消息来自 Docker 健康检查，携带完整时区。模拟器启动后生成的消息来自 `datetime.now().strftime("%z")`，naive datetime 的 `%z` 输出为空。因此 LogParser 同时遇到两种格式，引发混淆。

---

## 8. Python datetime.now() 生成 naive datetime 导致 %z 输出空白

### 症状

模拟器生成的日志行中时间戳结尾缺少 `+0000`：

```
# 修复前
192.168.10.142|-|[09/Jun/2026:10:43:24 ]|"GET /index HTTP/1.1"|200|...
                                                         ^ 只有空格，没有时区

# 修复后
192.168.10.142|-|[09/Jun/2026:10:43:24 +0000]|"GET /index HTTP/1.1"|200|...
```

### 原因

`log-simulator/simulator.py` 使用：

```python
now = datetime.now().strftime("%d/%b/%Y:%H:%M:%S %z")
```

`datetime.now()` 返回的是 **naive datetime**（无时区信息）。Python 的 `%z` 指令对 naive datetime 输出**空字符串**，导致时间戳末尾只有空格。

### 解决

使用 `datetime.now(timezone.utc)` 生成 timezone-aware 的 datetime：

```python
from datetime import datetime, timezone

now = datetime.now(timezone.utc).strftime("%d/%b/%Y:%H:%M:%S %z")
```

验证方式：
```python
>>> from datetime import datetime, timezone
>>> datetime.now(timezone.utc).strftime("%d/%b/%Y:%H:%M:%S %z")
'09/Jun/2026:10:45:19 +0000'
```

### 更新部署

修复后需要重启模拟器使之生效：

```bash
docker compose exec nginx sh -c "pkill -f simulator.py"
docker cp log-simulator/simulator.py nginx:/simulator.py
docker compose exec -d nginx sh -c "nohup python3 /simulator.py > /tmp/simulator.log 2>&1 &"
```

---

## 9. TumblingEventTimeWindows 需要显式分配 Event Time

### 症状

设置了 `TumblingEventTimeWindows.of(Time.minutes(1))`，但窗口**始终不触发**，所有窗口结果表（`pv_minute`, `uv_minute`, `status_minute`）持续为空，即使 `access_log_sample`（非窗口写入）已有数据。

### 原因

初始代码：

```java
DataStream<LogEvent> logStream = env
    .fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source")
    .map(new LogParser())
    .filter(e -> e != null);
// 后续直接使用 logStream 做窗口聚合
logStream.keyBy(...)
    .window(TumblingEventTimeWindows.of(Time.minutes(1)))
    .aggregate(...)
```

存在两个问题：

1. `WatermarkStrategy.noWatermarks()` 不生成任何 watermark。Event Time 窗口**依赖 watermark 来触发**，没有 watermark 意味着窗口永远不会结束。
2. 即使有了 watermark 策略，也需要从 `LogEvent` 中提取事件时间戳作为 Flink 的 Event Time。代码中从未调用 `assignTimestampsAndWatermarks()`。

### 解决

添加 `assignTimestampsAndWatermarks`，从 `LogEvent.timestamp` 中提取 Event Time：

```java
DataStream<LogEvent> logStream = env
    .fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source")
    .map(new LogParser())
    .filter(e -> e != null)
    .assignTimestampsAndWatermarks(
        WatermarkStrategy
            .<LogEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
            .withTimestampAssigner((event, timestamp) ->
                event.timestamp.toInstant(ZoneOffset.UTC).toEpochMilli()
            )
    );
```

`forBoundedOutOfOrderness(5s)` 允许 5 秒乱序到达的日志，平衡实时性与完整性。

> 注意：使用 Event Time 需要确保事件时间戳单调递增且与系统时钟大致同步。模拟器生成的日志使用当前 UTC 时间，满足此假设。对于乱序严重的生产场景，可能需要更大的容忍窗口（如 30 秒到 2 分钟）。

---

## 10. ProcessWindowFunction 缺失 — AggregateFunction 取不到窗口上下文

### 症状

`PvAggregator.PvResult`、`UvAggregator.UvResult` 等结果类中定义了 `windowStart` 和 `windowEnd` 字段，但写入 ClickHouse 时这些字段始终为 0。

### 原因

Flink 的 `AggregateFunction` 接口专注于增量聚合（维护累加器、合并部分结果），其 `getResult()` 方法**无法访问窗口的上下文信息**（窗口的开始时间、结束时间、分组的 key 等）。

因此聚合结果中的 `windowStart` 和 `windowEnd` 字段永远不会被赋值。

### 解决

为每个聚合函数配对对应的 `ProcessWindowFunction`（或 `ProcessAllWindowFunction`），用于在聚合结果产出后补充窗口上下文：

```java
// Keyed window → ProcessWindowFunction
public class PvWindowFunction
        extends ProcessWindowFunction<PvAggregator.PvResult, PvAggregator.PvResult, String, TimeWindow> {

    @Override
    public void process(String key, Context context,
                        Iterable<PvAggregator.PvResult> elements,
                        Collector<PvAggregator.PvResult> out) {
        PvAggregator.PvResult r = elements.iterator().next();
        r.windowStart = context.window().getStart();
        r.windowEnd = context.window().getEnd();
        r.url = key;
        out.collect(r);
    }
}

// 非 keyed window (windowAll) → ProcessAllWindowFunction
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
```

使用两参数版本的 `.aggregate()`：

```java
// AggregateFunction + ProcessWindowFunction
.aggregate(new PvAggregator(), new PvWindowFunction())

// 对于 windowAll 则用 ProcessAllWindowFunction
.aggregate(new UvAggregator(), new UvWindowFunction())
```

> `ProcessAllWindowFunction` 与 `ProcessWindowFunction` 的签名不同：前者没有 `KEY` 泛型参数，`process` 方法也没有 `key` 参数。

---

## 附录：验证检查清单

```bash
# 1. Flink 作业状态
docker exec flink-jobmanager flink list

# 2. Flink REST API 指标（确认数据流经所有算子）
curl -s "http://localhost:8081/jobs/<jobId>/vertices/<vertexId>/subtasks/0/metrics?get=\
Filter.numRecordsIn,Filter.numRecordsOut,log-sample-sink.numRecordsIn"

# 3. ClickHouse 数据写入
docker exec clickhouse clickhouse-client --query "SELECT count() FROM access_log_sample"
docker exec clickhouse clickhouse-client --query "SELECT url, pv_count FROM pv_minute ORDER BY pv_count DESC"
docker exec clickhouse clickhouse-client --query "SELECT uv_count FROM uv_minute"
docker exec clickhouse clickhouse-client --query "SELECT status_code, sum(count) FROM status_minute GROUP BY status_code"

# 4. Kafka 消费者 Lag（确认持续消费）
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:19092 \
    --group flink-analytics --describe

# 5. 模拟器运行状态
docker exec nginx sh -c "wc -l /var/log/nginx/access.log"
```
