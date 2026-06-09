# Phase 4 踩坑记录

> Spring Boot 3 后端服务 — 从 ClickHouse 查询数据并提供 REST API + WebSocket

---

## 目录

1. [clickhouse-jdbc 版本兼容性](#1-clickhouse-jdbc-版本兼容性)
2. [httpcore5 版本冲突](#2-httpcore5-版本冲突)
3. [HikariCP 不支持的数据源属性](#3-hikaricp-不支持的数据源属性)
4. [ClickHouse 用户认证与 JDBC URL](#4-clickhouse-用户认证与-jdbc-url)
5. [状态码分组 key 格式错误](#5-状态码分组-key-格式错误)
6. [Docker Hub 镜像拉取失败](#6-docker-hub-镜像拉取失败)
7. [Lombok @RequiredArgsConstructor 与构造器冲突](#7-lombok-requiredargsconstructor-与构造器冲突)

---

## 1. clickhouse-jdbc 版本兼容性

**症状：** 在 `pom.xml` 中复制文档的依赖 `clickhouse-jdbc:0.5.0`，编译成功但启动时 HikariCP 连接池无法建立连接。

**根因：** ClickHouse 26.5.1 使用的 HTTP 协议与 0.5.0 版本 JDBC 驱动的内部协议不兼容。这是 Phase 3 已遇到并解决过的相同问题。

**解决方式：** 将版本固定为 `0.9.8`：

```xml
<dependency>
    <groupId>com.clickhouse</groupId>
    <artifactId>clickhouse-jdbc</artifactId>
    <version>0.9.8</version>
</dependency>
```

## 2. httpcore5 版本冲突

**症状：** 启动时抛出 `NoSuchMethodError`：

```
java.lang.NoSuchMethodError:
'org.apache.hc.core5.net.URIBuilder org.apache.hc.core5.net.URIBuilder.optimize()'
```

**根因：** `clickhouse-jdbc:0.9.8` 调用 `URIBuilder.optimize()` 方法，该方法在 Apache HTTPCore 5.3 中引入。但 Spring Boot 3.3.0 的 BOM 管理的 `httpcore5` 版本为 `5.2.4`，缺少此方法。

**解决方式：** 在 `pom.xml` 中显式引入高版本覆盖 Parent 管理的版本：

```xml
<dependency>
    <groupId>org.apache.httpcomponents.core5</groupId>
    <artifactId>httpcore5</artifactId>
    <version>5.3.1</version>
</dependency>
```

## 3. HikariCP 不支持的数据源属性

**症状：** 启动日志中出现 `ClientMisconfigurationException`：

```
Caused by: com.clickhouse.client.api.ClientMisconfigurationException:
Unknown and unmapped config properties: [useServerPrepStmts]
```

**根因：** Phase 3 的经验中 Flink 场景需要 `useServerPrepStmts=false`，但在 Spring Boot 中错误地将此 ClickHouse 不认识的属性传给了 `HikariConfig.addDataSourceProperty()`。`clickhouse-jdbc:0.9.8` 的 HTTP 客户端会校验全部传入的配置项，遇到不认识的 key 直接报错。

**解决方式：** 删除该属性配置：

```java
// 错误写法
config.addDataSourceProperty("useServerPrepStmts", "false");

// 正确写法 — 直接删除，clickhouse-jdbc 会自动选择最优方式
HikariConfig config = new HikariConfig();
config.setJdbcUrl(jdbcUrl);
config.setDriverClassName(driverClass);
config.setMaximumPoolSize(10);
config.setMinimumIdle(2);
config.setConnectionTimeout(5000);
```

**类型：** 配置错误

## 4. ClickHouse 用户认证与 JDBC URL

**症状：** 连接 ClickHouse 时认证失败或 `password` 参数被拒。

**根因（两个子问题）：**

1. ClickHouse 26.x 限制 `default` 用户只能从 localhost 访问（已在 Phase 1 初始化中创建了 `analytics` 无密码用户）。
2. `clickhouse-jdbc:0.9.8` 行为变更：当 URL 中包含 `&password=` 空值时，驱动会拒绝连接。

**解决方式：**

- JDBC URL 使用 `analytics` 用户，省略 `password` 参数：
  ```
  jdbc:clickhouse://clickhouse:8123/default?user=analytics
  ```
- 配置文件中采用双 profile 模式：
  - `default`（本地运行）：`localhost:8123`
  - `docker`（容器运行）：`clickhouse:8123`

## 5. 状态码分组 key 格式错误

**症状：** `/api/overview` 返回的状态码分布 key 为 `"200xx": 221, "400xx": 9` 而非预期的 `"2xx": 221, "4xx": 9`。

**根因：** SQL 中的 ClickHouse 算术写错：

```sql
-- 错误：200 / 100 * 100 = 200 → "200xx"
SELECT toString(intDiv(status_code, 100) * 100) || 'xx'

-- 正确：200 / 100 = 2 → "2xx"
SELECT toString(intDiv(status_code, 100)) || 'xx'
```

`status_code` 在 `status_minute` 表中存储的是具体状态码（200/404/500），不是已分组的值。所以 `intDiv(200) * 100` 得到 `200` 而非 `2`。

**解决方式：** 移除 `* 100`，只需 `intDiv` 取百位数字。

## 6. Docker Hub 镜像拉取失败

**症状：** `docker compose build server` 失败：

```
failed to fetch oauth token: dial tcp 185.60.216.50:443: connectex: A connection attempt failed
```

**根因：** 环境在中国网络环境，Docker Hub 无法直连（GFW 限制）。系统中没有现成的 JDK 17 JRE 基础镜像。

**解决方式：**

- 保留 `Dockerfile` 和 `docker-compose.yml` 中的 server 服务定义
- 待有 Docker Hub 访问时，用户可执行 `docker compose build server` 构建
- 当前直接在宿主机通过 `java -jar` 运行，已验证全部 API 正常

## 7. Lombok @RequiredArgsConstructor 与构造器冲突

**症状（潜在问题）：** 文档中 `AnalyticsService` 同时使用了 `@RequiredArgsConstructor` 和手动构造器，导致二义性告警。

**根因：** `AnalyticsService` 注入了非 Spring 管理的 `JdbcTemplate`（通过 `new JdbcTemplate(dataSource)` 创建），不能使用 `@RequiredArgsConstructor` 注入。

**解决方式：** 移除 `@RequiredArgsConstructor`，保留明确的构造器：

```java
@Service
public class AnalyticsService {
    private final JdbcTemplate clickhouseJdbc;

    // 用 @Value 注入实际 JDBC URL（而非 @Autowired DataSource），
    // 构造器私有逻辑：从 DataSource 创建 JdbcTemplate
    public AnalyticsService(DataSource clickhouseDataSource) {
        this.clickhouseJdbc = new JdbcTemplate(clickhouseDataSource);
    }
}
```

---

## 附：Phase 4 验证项

- [x] `mvn clean package` 编译通过（零错误）
- [x] 服务在 2.1 秒内无报错启动完成
- [x] `GET /api/health` → `ok`
- [x] `GET /api/overview` → 真实 PV/UV/状态码/响应时间
- [x] `GET /api/pv/trend?minutes=30` → 时间序列数据
- [x] `GET /api/pv/top?limit=5` → 页面排行
- [x] `GET /api/referer` → 来源类型分布
- [x] `GET /api/funnel` → 空数据（Phase 3 未写入）
- [x] WebSocket `/ws` → STOMP 协议可连接
- [x] Dockerfile 及 docker-compose 配置已就绪
