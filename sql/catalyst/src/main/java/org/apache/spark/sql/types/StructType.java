package org.apache.spark.sql.types;

import lombok.Data;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by kenya on 2019/1/19.
 */
@Data
public class StructType extends DataType {

    List<StructField> fields;

    public StructType(List<StructField> fields){
        this.fields = fields;
    }

    public StructType(StructField...fields){
        this(Arrays.asList(fields));
    }

    public StructType(){
        this(new ArrayList<>());
    }

    public List<AttributeReference> toAttributes(){
        List<AttributeReference> references = new ArrayList<>();
        for(StructField f: fields){
            references.add(new AttributeReference(f.name, f.dataType, f.nullable, f.metadata));
        }
        return references;
    }

    public StructType add(String name, DataType dataType){
        fields.add(new StructField(name, dataType, true, Metadata.empty()));
        return new StructType(fields);
    }

    public StructType add(
            String name,
            DataType dataType,
            boolean nullable,
            String comment){
        fields.add(new StructField(name, dataType, nullable).withComment(comment));
        return new StructType(fields);
    }

    @Override
    public boolean equals(Object o){
        boolean ok = super.equals(o);
        if(!ok){
            return false;
        }
        StructType s =(StructType)o;
        if((fields==null || fields.size()==0) && (s.fields==null && s.fields.size()==0)){
            return true;
        }

        if(s.fields.size()!=this.fields.size()){
            return false;
        }

        for(int i=0;i<fields.size(); i++){
            ok = equals(fields.get(i), s.fields.get(i));
            if(!ok){
                return false;
            }
        }
        return true;


    }

}
