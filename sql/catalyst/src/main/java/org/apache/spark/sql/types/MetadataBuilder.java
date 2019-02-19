package org.apache.spark.sql.types;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by kenya on 2019/1/19.
 */
public class MetadataBuilder {
    Map<String, Object> map = new HashMap<>();

    public MetadataBuilder putString(String key, String value){
            return put(key, value);
    }

    private MetadataBuilder put(String key, Object value){
        map.put(key, value);
        return this;
    }

    public Metadata build(){
        return new Metadata(map);
    }

}
