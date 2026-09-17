package com.company.shopinsight.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;

import javax.sql.DataSource;
import java.sql.Driver;

/**
 * ClickHouse 数据源配置。
 *
 * <p>手动创建 DataSource 和 JdbcTemplate，而不是用 {@code spring.datasource} 自动配置。
 * 原因：项目后续要同时连 ClickHouse 和 MySQL 两个数据源，各自独立的 JdbcTemplate
 * 更清晰，避免和 Spring Boot 的单数据源自动配置纠缠。
 *
 * <p>用 {@link SimpleDriverDataSource}（每次取连接新建一个）而不是连接池：
 * ClickHouse 是无事务的 OLAP 数据库，连接池的意义不大，而且避免 HikariCP
 * 对它做事务相关的探测。
 */
@Configuration
public class ClickHouseConfig {

    @Value("${clickhouse.url}")
    private String url;

    @Value("${clickhouse.username}")
    private String username;

    @Value("${clickhouse.password}")
    private String password;

    @Value("${clickhouse.driver-class-name}")
    private String driverClassName;

    @Bean
    public DataSource clickHouseDataSource() throws Exception {
        SimpleDriverDataSource dataSource = new SimpleDriverDataSource();
        dataSource.setDriverClass((Class<? extends Driver>) Class.forName(driverClassName));
        dataSource.setUrl(url);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        return dataSource;
    }

    @Bean
    public JdbcTemplate clickHouseJdbcTemplate(DataSource clickHouseDataSource) {
        return new JdbcTemplate(clickHouseDataSource);
    }
}
