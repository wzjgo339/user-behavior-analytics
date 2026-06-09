# Phase 4：后端服务（Spring Boot 3）

## 目标

创建 Spring Boot 3 后端，提供 REST API + WebSocket，从 ClickHouse 查询数据并推送到前端。

---

## 步骤 4.1：创建 Spring Boot 项目

```bash
mkdir -p server/src/main/java/com/analytics/server
mkdir -p server/src/main/resources
mkdir -p server/src/main/java/com/analytics/server/controller
mkdir -p server/src/main/java/com/analytics/server/service
mkdir -p server/src/main/java/com/analytics/server/model
mkdir -p server/src/main/java/com/analytics/server/config
```

`server/pom.xml`：

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-parent</artifactId>
        <version>3.3.0</version>
        <relativePath/>
    </parent>

    <groupId>com.analytics</groupId>
    <artifactId>analytics-server</artifactId>
    <version>1.0.0</version>
    <name>analytics-server</name>

    <properties>
        <java.version>17</java.version>
    </properties>

    <dependencies>
        <!-- Web -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
        </dependency>
        <!-- WebSocket -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-websocket</artifactId>
        </dependency>
        <!-- ClickHouse JDBC -->
        <dependency>
            <groupId>com.clickhouse</groupId>
            <artifactId>clickhouse-jdbc</artifactId>
            <version>0.5.0</version>
        </dependency>
        <!-- Redis -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-data-redis</artifactId>
        </dependency>
        <!-- 工具 -->
        <dependency>
            <groupId>org.projectlombok</groupId>
            <artifactId>lombok</artifactId>
            <optional>true</optional>
        </dependency>
        <dependency>
            <groupId>com.fasterxml.jackson.core</groupId>
            <artifactId>jackson-databind</artifactId>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
                <configuration>
                    <excludes>
                        <exclude><groupId>org.projectlombok</groupId><artifactId>lombok</artifactId></exclude>
                    </excludes>
                </configuration>
            </plugin>
        </plugins>
    </build>
</project>
```

---

## 步骤 4.2：配置文件

`server/src/main/resources/application.yml`：

```yaml
server:
  port: 8080

spring:
  application:
    name: analytics-server
  # Redis
  data:
    redis:
      host: localhost
      port: 6379
      timeout: 5000

clickhouse:
  jdbc-url: jdbc:clickhouse://localhost:8123/default
  driver-class: com.clickhouse.jdbc.ClickHouseDriver

# 聚合数据推送间隔（毫秒）
analytics:
  push-interval: 5000
  # 在 WebSocket 中每隔 N 毫秒推送一次聚合数据
```

---

## 步骤 4.3：ClickHouse 配置类

`server/src/main/java/com/analytics/server/config/ClickHouseConfig.java`：

```java
package com.analytics.server.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

@Configuration
public class ClickHouseConfig {

    @Bean
    public DataSource clickhouseDataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:clickhouse://localhost:8123/default");
        config.setDriverClassName("com.clickhouse.jdbc.ClickHouseDriver");
        config.setMaximumPoolSize(10);
        config.setMinimumIdle(2);
        return new HikariDataSource(config);
    }
}
```

---

## 步骤 4.4：WebSocket 配置

`server/src/main/java/com/analytics/server/config/WebSocketConfig.java`：

```java
package com.analytics.server.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;

@Configuration
@EnableWebSocketMessageBroker
public class WebSocketConfig implements WebSocketMessageBrokerConfigurer {

    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry) {
        // 前端连接端点
        registry.addEndpoint("/ws").setAllowedOriginPatterns("*").withSockJS();
    }

    @Override
    public void configureMessageBroker(org.springframework.messaging.simp.config.MessageBrokerRegistry registry) {
        // 后端推送地址前缀
        registry.enableSimpleBroker("/topic");
        // 前端发消息的前缀
        registry.setApplicationDestinationPrefixes("/app");
    }
}
```

---

## 步骤 4.5：数据模型

`server/src/main/java/com/analytics/server/model/RealtimeData.java`：

```java
package com.analytics.server.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import java.util.List;
import java.util.Map;

/** WebSocket 实时推送的数据结构 */
@Data
@AllArgsConstructor
public class RealtimeData {
    private long timestamp;
    private long pv;
    private long uv;
    private List<Map<String, Object>> topPages;     // [{url, pv}]
    private Map<String, Long> statusCodes;           // {"2xx": N, "3xx": N, ...}
    private double avgResponseTime;
}
```

---

## 步骤 4.6：数据服务层

`server/src/main/java/com/analytics/server/service/AnalyticsService.java`：

```java
package com.analytics.server.service;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import javax.sql.DataSource;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class AnalyticsService {

    private final JdbcTemplate clickhouseJdbc;

    public AnalyticsService(DataSource clickhouseDataSource) {
        this.clickhouseJdbc = new JdbcTemplate(clickhouseDataSource);
    }

    /** 今日总 PV */
    public long getTodayPv() {
        String sql = "SELECT sum(pv_count) FROM pv_minute " +
                     "WHERE window_start >= toDate(now())";
        Long result = clickhouseJdbc.queryForObject(sql, Long.class);
        return result != null ? result : 0;
    }

    /** 今日总 UV */
    public long getTodayUv() {
        String sql = "SELECT sum(uv_count) FROM uv_minute " +
                     "WHERE window_start >= toDate(now())";
        Long result = clickhouseJdbc.queryForObject(sql, Long.class);
        return result != null ? result : 0;
    }

    /** TOP N 热门页面 */
    public List<Map<String, Object>> getTopPages(int limit) {
        String sql = "SELECT url, sum(pv_count) as pv FROM pv_minute " +
                     "WHERE window_start >= toDate(now()) " +
                     "GROUP BY url ORDER BY pv DESC LIMIT ?";
        return clickhouseJdbc.queryForList(sql, limit);
    }

    /** 状态码分布 */
    public Map<String, Long> getStatusDistribution() {
        String sql = "SELECT multiIf(status >= 200 AND status < 300, '2xx', " +
                     "          status >= 300 AND status < 400, '3xx', " +
                     "          status >= 400 AND status < 500, '4xx', " +
                     "          '5xx') as code_range, " +
                     "       count() as cnt " +
                     "FROM access_log_sample " +
                     "WHERE timestamp >= now() - INTERVAL 5 MINUTE " +
                     "GROUP BY code_range";
        List<Map<String, Object>> rows = clickhouseJdbc.queryForList(sql);
        // 转成 Map 返回
        return Map.of(
            "2xx", rows.stream().filter(r -> "2xx".equals(r.get("code_range"))).mapToLong(r -> (Long)r.get("cnt")).sum(),
            "3xx", rows.stream().filter(r -> "3xx".equals(r.get("code_range"))).mapToLong(r -> (Long)r.get("cnt")).sum(),
            "4xx", rows.stream().filter(r -> "4xx".equals(r.get("code_range"))).mapToLong(r -> (Long)r.get("cnt")).sum(),
            "5xx", rows.stream().filter(r -> "5xx".equals(r.get("code_range"))).mapToLong(r -> (Long)r.get("cnt")).sum()
        );
    }

    /** 最近 N 分钟的 PV 时间序列 */
    public List<Map<String, Object>> getPvTrend(int minutes) {
        String sql = "SELECT toStartOfMinute(window_start) as t, " +
                     "       sum(pv_count) as pv " +
                     "FROM pv_minute " +
                     "WHERE window_start >= now() - INTERVAL ? MINUTE " +
                     "GROUP BY t ORDER BY t";
        return clickhouseJdbc.queryForList(sql, minutes);
    }

    /** 来源分析 */
    public List<Map<String, Object>> getRefererAnalysis() {
        String sql = "SELECT referer_type, sum(count) as cnt " +
                     "FROM referer_hourly " +
                     "WHERE window_start >= toDate(now()) " +
                     "GROUP BY referer_type";
        return clickhouseJdbc.queryForList(sql);
    }

    /** 漏斗分析 */
    public List<Map<String, Object>> getFunnelData() {
        String sql = "SELECT step_name, user_count, conversion_rate " +
                     "FROM funnel_analysis " +
                     "WHERE window_start >= toDate(now()) " +
                     "ORDER BY step_order";
        return clickhouseJdbc.queryForList(sql);
    }

    // 更多查询方法按需添加...
}
```

---

## 步骤 4.7：REST Controller

`server/src/main/java/com/analytics/server/controller/AnalyticsController.java`：

```java
package com.analytics.server.controller;

import com.analytics.server.service.AnalyticsService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class AnalyticsController {

    private final AnalyticsService service;

    /** 概览数据 */
    @GetMapping("/overview")
    public Map<String, Object> overview() {
        return Map.of(
            "pv", service.getTodayPv(),
            "uv", service.getTodayUv(),
            "topPages", service.getTopPages(10),
            "statusCodes", service.getStatusDistribution()
        );
    }

    /** PV 趋势 */
    @GetMapping("/pv/trend")
    public List<Map<String, Object>> pvTrend(
            @RequestParam(defaultValue = "60") int minutes) {
        return service.getPvTrend(minutes);
    }

    /** TOP 页面 */
    @GetMapping("/pv/top")
    public List<Map<String, Object>> topPages(
            @RequestParam(defaultValue = "10") int limit) {
        return service.getTopPages(limit);
    }

    /** 来源分析 */
    @GetMapping("/referer")
    public List<Map<String, Object>> referer() {
        return service.getRefererAnalysis();
    }

    /** 漏斗数据 */
    @GetMapping("/funnel")
    public List<Map<String, Object>> funnel() {
        return service.getFunnelData();
    }

    /** 健康检查 */
    @GetMapping("/health")
    public String health() { return "ok"; }
}
```

---

## 步骤 4.8：WebSocket 定时推送

`server/src/main/java/com/analytics/server/service/RealtimePushService.java`：

```java
package com.analytics.server.service;

import com.analytics.server.model.RealtimeData;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class RealtimePushService {

    private final SimpMessagingTemplate messagingTemplate;
    private final AnalyticsService analyticsService;

    /** 每 5 秒推送一次实时数据 */
    @Scheduled(fixedRateString = "${analytics.push-interval:5000}")
    public void pushRealtimeData() {
        try {
            RealtimeData data = new RealtimeData(
                System.currentTimeMillis(),
                analyticsService.getTodayPv(),
                analyticsService.getTodayUv(),
                analyticsService.getTopPages(10),
                analyticsService.getStatusDistribution(),
                0.045  // 平均响应时间，可从 ClickHouse 查询
            );
            messagingTemplate.convertAndSend("/topic/realtime", data);
        } catch (Exception e) {
            log.error("推送实时数据失败", e);
        }
    }
}
```

别忘了在主类开启定时任务：

```java
@SpringBootApplication
@EnableScheduling
public class AnalyticsServerApplication {
    public static void main(String[] args) {
        SpringApplication.run(AnalyticsServerApplication.class, args);
    }
}
```

---

## 步骤 4.9：配置 CORS（解决前端跨域）

`server/src/main/java/com/analytics/server/config/CorsConfig.java`：

```java
package com.analytics.server.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;
import org.springframework.web.filter.CorsFilter;

@Configuration
public class CorsConfig {

    @Bean
    public CorsFilter corsFilter() {
        CorsConfiguration config = new CorsConfiguration();
        config.addAllowedOriginPattern("*");
        config.addAllowedHeader("*");
        config.addAllowedMethod("*");
        config.setAllowCredentials(true);

        UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
        source.registerCorsConfiguration("/**", config);
        return new CorsFilter(source);
    }
}
```

---

## 步骤 4.10：启动验证

```bash
cd server
mvn spring-boot:run

# 验证 API
curl http://localhost:8080/api/health
# 返回: ok

curl http://localhost:8080/api/overview
# 返回: {"pv": 1234, "uv": 567, ...}
```

---

## 本阶段完成标志

- [ ] `mvn spring-boot:run` 启动无报错
- [ ] `GET /api/health` 返回 `ok`
- [ ] `GET /api/overview` 返回 ClickHouse 中的真实数据
- [ ] `GET /api/pv/trend` 返回时间序列数据
- [ ] WebSocket 可连接（浏览器控制台测试或用 wscat）
