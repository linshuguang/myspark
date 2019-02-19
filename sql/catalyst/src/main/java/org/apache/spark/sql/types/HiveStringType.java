package org.apache.spark.sql.types;

import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * Created by kenya on 2019/1/19.
 */
public class HiveStringType {

    public static DataType replaceCharType(DataType dt){

        if(dt instanceof ArrayType){
            ArrayType arrayType = (ArrayType)dt;
            return new ArrayType(replaceCharType(arrayType.getElementType()), arrayType.isContainsNull());
        }else if(dt instanceof MapType) {
            MapType mapType = (MapType) dt;
            return new MapType(replaceCharType(mapType.getKeyType()), replaceCharType(mapType.getValueType()), mapType.isContainsNull());
        }else if(dt instanceof StructType){
            StructType structType = (StructType)dt;
            List<StructField>  fields = structType.getFields();
            for(StructField field: fields){
                field.setDataType(replaceCharType(field.getDataType()));
            }
        }else if( StringUtils.equals(dt.getClass().getSimpleName(), HiveStringType.class.getSimpleName())){
            return new StringType();
        }
        return dt;
    }

}
