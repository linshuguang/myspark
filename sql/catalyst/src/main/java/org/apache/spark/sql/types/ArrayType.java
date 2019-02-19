package org.apache.spark.sql.types;

import lombok.Data;

/**
 * Created by kenya on 2019/1/19.
 */
@Data
public class ArrayType extends DataType{
    DataType elementType;
    boolean containsNull;

    public ArrayType(DataType elementType, boolean containsNull){
        this.elementType = elementType;
        this.containsNull = containsNull;
    }

    public ArrayType(DataType elementType){
        this(elementType, true);
    }

    public ArrayType(){
        this(new NullType(), true);
    }



}
