package org.apache.spark.sql.types;

import lombok.Data;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;

import java.util.ArrayList;
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

    public List<AttributeReference> toAttributes(){
        List<AttributeReference> references = new ArrayList<>();
        for(StructField f: fields){
            references.add(new AttributeReference(f.name, f.dataType, f.nullable, f.metadata));
        }
        return references;
    }

}
