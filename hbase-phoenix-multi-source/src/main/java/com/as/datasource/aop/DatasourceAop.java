package com.as.datasource.aop;

import com.as.datasource.DatasourceNames;
import com.as.datasource.annotation.Datasource;
import com.as.datasource.config.DynamicDatasource;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;

@Aspect
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
@Slf4j
public class DatasourceAop {

    @Pointcut("@annotation(com.as.datasource.annotation.Datasource)" + "@within(com.as.datasource.annotation.Datasource)")
    public void datasourcePointcut() {}

    @Around("datasourcePointcut()")
    public Object around(ProceedingJoinPoint point) throws Throwable {
        MethodSignature signature = (MethodSignature) point.getSignature();
        Class<?> target = point.getTarget().getClass();
        Method method = signature.getMethod();
        Datasource targetDatasource = target.getAnnotation(Datasource.class);
        Datasource methodDatasource = method.getAnnotation(Datasource.class);
        if(null != targetDatasource || null != methodDatasource) {
            String value = null != methodDatasource ? methodDatasource.value() : targetDatasource.value();
            DynamicDatasource.setDatasource(value);
            log.info("set datasource is {}", value);
        }else {
            DynamicDatasource.setDatasource(DatasourceNames.FIRST);
            log.info("set datasource is {}", DatasourceNames.FIRST);
        }
        try {
            return point.proceed();
        }finally {
            DynamicDatasource.clearDatasource();
            log.info("clear datasource");
        }
    }
}
