package org.apache.spark.util;

import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactoryUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * Created by kenya on 2019/3/6.
 */
@Service
public class BeanLoader implements ApplicationContextAware {
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(BeanLoader.class);

    private static ApplicationContext applicationContext;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        BeanLoader.applicationContext = applicationContext;
    }

    private static ApplicationContext getApplicationContext() {
        return applicationContext;
    }

    public static <T> T getBean(Class<T> c) {
        return (T) BeanFactoryUtils.beanOfTypeIncludingAncestors(getApplicationContext(), c);
    }

    public static <T> T getBeanByName(String name,Class<T> c){
        return (T) applicationContext.getBean(name,c);
    }


    public static  <T> Map<String, T> getBeans(Class<T> c) {
        Map<String, T> beans = BeanFactoryUtils.beansOfTypeIncludingAncestors(getApplicationContext(),c);
        return (Map<String, T>)beans;
    }
}
