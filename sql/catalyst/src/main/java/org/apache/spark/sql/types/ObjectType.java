package org.apache.spark.sql.types;

import lombok.Data;

/**
 * Created by kenya on 2019/1/23.
 */
@Data
public class ObjectType extends DataType {
    Class cls;

    public ObjectType(Class cls){
        this.cls = cls;
    }

}
