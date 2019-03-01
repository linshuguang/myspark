package org.apache.spark.sql.internal.config;

import java.util.Map;

/**
 * Created by kenya on 2019/2/27.
 */
public class MapProvider implements ConfigProvider {
    Map<String, String> conf;

    public MapProvider(Map<String, String> conf){
        this.conf = conf;
    }

    @Override
    public String get(String key){
        return conf.get(key);
    }
}
