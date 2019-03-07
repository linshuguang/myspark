package org.apache.spark.sql.types;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.catalyst.parser.ParserUtils;

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

    @Override
    public boolean equals(Object o){
        boolean ok = false;
        if(this==o){
            return true;
        }
        if(o==null){
            return false;
        }
        if(this.getClass()!=o.getClass()){
            return false;
        }
        StructField s= (StructField)o;

        if(!StringUtils.equals(name,s.name)){
            return false;
        }

        if(dataType!=null){
            ok = dataType.equals(s.dataType);
        }else if(s.dataType!=null){
            ok = s.dataType.equals(this.dataType);
        }
        if(!ok){
            return false;
        }
        ok = this.nullable == s.nullable;
        if(!ok){
            return false;
        }
        return true;
    }

}
