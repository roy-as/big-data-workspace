package com.as.datasource.config;

import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource;

import javax.sql.DataSource;
import java.util.Map;

/**
 * 数据源
 */
public class DynamicDatasource extends AbstractRoutingDataSource {

    private static final ThreadLocal<String> CONTEXT_HOLDER;

    static {
        CONTEXT_HOLDER = new ThreadLocal<>();
    }

    public DynamicDatasource(DataSource defaultDatasource, Map<Object, Object> targetDatasources) {
        super.setDefaultTargetDataSource(defaultDatasource);
        super.setTargetDataSources(targetDatasources);
        super.afterPropertiesSet();
    }

    @Override
    protected Object determineCurrentLookupKey() {
        return getDatasource();
    }

    public static void setDatasource(String datasourceName) {
        CONTEXT_HOLDER.set(datasourceName);
    }

    public static String getDatasource() {
        return CONTEXT_HOLDER.get();
    }

    public static void clearDatasource() {
        CONTEXT_HOLDER.remove();
    }
}
