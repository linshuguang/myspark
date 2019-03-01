package org.apache.spark.sql.internal.config;

/**
 * Created by kenya on 2019/2/27.
 */
public interface ConfigProvider {
    String get(String key);
}
