package com.as.utils;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

@Component
public class SpringContextUtil implements ApplicationContextAware {

    private static ApplicationContext context;


    public static void setContext(ApplicationContext context) {
        SpringContextUtil.context = context;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        SpringContextUtil.context = applicationContext;
    }

    public static <T> T getBean(Class<T> clazz) {
        return SpringContextUtil.context.getBean(clazz);
    }

}
