package org.apache.spark.sql.catalyst.expressions;

import org.apache.spark.sql.types.DataType;

/**
 * Created by kenya on 2019/2/14.
 */
public class Cast extends UnaryExpression{
    DataType dataType;
    String timeZoneId;

    public Cast(Expression child,DataType dataType){
        this(child, dataType, null);
    }

    public Cast(Expression child,DataType dataType, String timeZoneId){
        super(child);
        this.dataType = dataType;
        this.timeZoneId = timeZoneId;
    }



}
