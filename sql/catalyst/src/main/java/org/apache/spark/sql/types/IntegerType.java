package org.apache.spark.sql.types;

/**
 * Created by kenya on 2019/1/22.
 */
public class IntegerType extends DataType {
    @Override
    public boolean equals(Object o){
        return o instanceof  IntegerType;
    }
}
