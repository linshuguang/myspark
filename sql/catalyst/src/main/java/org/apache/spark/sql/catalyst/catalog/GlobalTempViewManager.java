package org.apache.spark.sql.catalyst.catalog;

import lombok.Data;

/**
 * Created by kenya on 2019/2/26.
 */
@Data
public class GlobalTempViewManager {
    String database;
    public GlobalTempViewManager(String database){
        this.database = database;
    }
}
