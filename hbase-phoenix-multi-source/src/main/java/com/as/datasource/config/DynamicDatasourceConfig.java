package com.as.datasource.config;

import com.alibaba.druid.filter.Filter;
import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.spring.boot.autoconfigure.DruidDataSourceBuilder;
import com.alibaba.druid.wall.WallConfig;
import com.alibaba.druid.wall.WallFilter;
import com.as.datasource.DatasourceNames;
import com.google.common.collect.ImmutableList;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
@Slf4j
public class DynamicDatasourceConfig {

    @Value("${sql.multi-query.datasource}")
    private String multiQueryDatasource;

    @Bean
    @ConfigurationProperties(prefix = "spring.datasource.druid.first")
    public DruidDataSource firstDataSource() {
        return DruidDataSourceBuilder.create().build();
    }

    @Bean
    @ConfigurationProperties(prefix = "spring.datasource.druid.second")
    public DruidDataSource secondDataSource() {
        return DruidDataSourceBuilder.create().build();
    }

    @Bean
    @Primary
    public DynamicDatasource datasource(DruidDataSource firstDataSource, DruidDataSource secondDataSource) {
        String[] datasourceNames = multiQueryDatasource.split(",");
        for (String datasourceName : datasourceNames) {
            switch (datasourceName) {
                case DatasourceNames.FIRST:
                    multiQuery(DatasourceNames.FIRST, firstDataSource);
                    break;
                case DatasourceNames.SECOND:
                    multiQuery(DatasourceNames.SECOND, secondDataSource);
                    break;
                default:
                    throw new RuntimeException("数据源不存在");
            }
        }
        Map<Object, Object> targetDataSources = new HashMap<>();
        targetDataSources.put(DatasourceNames.FIRST, firstDataSource);
        targetDataSources.put(DatasourceNames.SECOND, secondDataSource);
        return new DynamicDatasource(firstDataSource, targetDataSources);
    }

    private void multiQuery(String datasourceName, DruidDataSource dataSource) {
        WallConfig wallConfig = new WallConfig();
        wallConfig.setMultiStatementAllow(true);
        WallFilter wallFilter = new WallFilter();
        wallFilter.setConfig(wallConfig);
        List<Filter> filters = ImmutableList.of(wallFilter);
        dataSource.setProxyFilters(filters);
        try {
            dataSource.init();
        } catch (Exception e) {
            log.error("设置数据源:{}批处理失败", datasourceName, e);
        }

    }

}
