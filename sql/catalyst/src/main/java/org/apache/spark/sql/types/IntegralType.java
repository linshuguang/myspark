package org.apache.spark.sql.types;

/**
 * Created by kenya on 2019/1/22.
 */
public class IntegralType extends NumericType {
    @Override
    public boolean equals(Object o){
        return o instanceof  IntegralType;
    }
}
