package com.analytics.server.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

/**
 * ClickHouse 数据源配置（HikariCP 连接池）
 *
 * 注意：clickhouse-jdbc 0.9.8 依赖 Apache HC5，
 * 若 NoClassDefFoundError 出现，驱动会回退到 HTTP_URL_CONNECTION，不影响正常使用。
 */
@Configuration
public class ClickHouseConfig {

    @Value("${clickhouse.jdbc-url}")
    private String jdbcUrl;

    @Value("${clickhouse.driver-class}")
    private String driverClass;

    @Bean
    public DataSource clickhouseDataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setDriverClassName(driverClass);
        config.setMaximumPoolSize(10);
        config.setMinimumIdle(2);
        config.setConnectionTimeout(5000);
        config.setValidationTimeout(3000);
        return new HikariDataSource(config);
    }
}
