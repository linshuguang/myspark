package org.apache.spark.sql.types.HiveStringTypes;

/**
 * Created by kenya on 2019/2/26.
 */
public class VarcharType  extends HiveStringType {
    int length;
    public VarcharType(int length){
        this.length = length;
    }
}