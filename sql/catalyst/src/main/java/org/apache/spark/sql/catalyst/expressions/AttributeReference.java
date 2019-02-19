package org.apache.spark.sql.catalyst.expressions;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Metadata;

/**
 * Created by kenya on 2019/2/14.
 */
public class AttributeReference extends Attribute {
    String name;
    DataType dataType;
    boolean nullable;
    Metadata metadata ;

    public AttributeReference(String name, DataType dataType, boolean nullable, Metadata metadata){
        this.name = name;
        this.dataType = dataType;
        this.nullable = nullable;
        this.metadata = metadata;
    }

    public AttributeReference(String name, DataType dataType, boolean nullable){
        this(name, dataType, nullable, new Metadata());
    }

    public AttributeReference(String name, DataType dataType){
        this(name, dataType, true, new Metadata());
    }
}
