package org.apache.spark.sql.types;

/**
 * Created by kenya on 2019/1/22.
 */
public class TimestampType extends AtomicType {

    @Override
    public boolean equals(Object o){
        return o instanceof  TimestampType;
    }
}
