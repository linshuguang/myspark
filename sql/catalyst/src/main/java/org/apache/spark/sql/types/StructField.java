package org.apache.spark.sql.types;

import lombok.Data;

/**
 * Created by kenya on 2019/1/19.
 */
@Data
public class StructField {
    String name;
    DataType dataType;
    boolean nullable;
    Metadata metadata;

    public StructField(String name, DataType dataType, boolean nullable, Metadata metadata){
        this.name = name;
        this.dataType = dataType;
        this.nullable = nullable;
        this.metadata = metadata;
    }

    public StructField(String name, DataType dataType){
        this(name, dataType,true, new Metadata());
    }

}
