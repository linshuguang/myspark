package org.apache.spark.sql.catalyst.catalog;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by kenya on 2019/3/1.
 */
public class CatalogTablePartition {
    Map<String, String> spec;
    CatalogStorageFormat storage;
    Map<String, String> parameters;
    long createTime;
    long lastAccessTime;
    CatalogStatistics stats;
    public CatalogTablePartition(Map<String, String> spec,
            CatalogStorageFormat storage,
            Map<String, String> parameters,
            long createTime,
            long lastAccessTime,
            CatalogStatistics stats){
        this.spec = spec;
        this.storage = storage;
        this.parameters = parameters;
        this.createTime = createTime;
        this.lastAccessTime = lastAccessTime;
        this.stats = stats;
    }
    public CatalogTablePartition(Map<String, String> spec,
                                 CatalogStorageFormat storage){
        this(spec,storage,new HashMap<>(),System.currentTimeMillis(),-1,null);
    }

}
