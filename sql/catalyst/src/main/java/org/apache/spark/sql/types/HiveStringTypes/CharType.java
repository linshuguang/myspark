package org.apache.spark.sql.types.HiveStringTypes;

/**
 * Created by kenya on 2019/2/26.
 */
public class CharType extends HiveStringType {
    int length;
    public CharType(int length){
        this.length = length;
    }
}
