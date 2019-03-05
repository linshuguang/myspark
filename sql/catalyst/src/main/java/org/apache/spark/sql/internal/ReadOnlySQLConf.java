package org.apache.spark.sql.internal;

import org.apache.spark.TaskContext;

/**
 * Created by kenya on 2019/3/4.
 */
public class ReadOnlySQLConf extends SQLConf  {
    TaskContext context;

    public ReadOnlySQLConf(TaskContext context){
        this.context = context;
    }

}
