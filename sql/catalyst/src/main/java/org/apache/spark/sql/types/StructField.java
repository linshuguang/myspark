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

    public StructField(String name, DataType dataType, boolean nullable){
        this(name, dataType,nullable, new Metadata());
    }

    public StructField withComment(String comment){
        MetadataBuilder builder = new MetadataBuilder();
        builder.withMetadata(metadata);
        builder.putString("comment", comment);
        Metadata newMetadata = builder.build();
        metadata = newMetadata;
        return this;
    }

}
