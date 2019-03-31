package org.apache.spark.sql.catalyst.expressions;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.catalyst.parser.ParserUtils;
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


    @Override
    public boolean equals(Object o){
        if(o instanceof Cast){
            Cast c = (Cast)o;
            return ParserUtils.equals(dataType,c.dataType) && StringUtils.equals(timeZoneId,c.timeZoneId);
        }
        return false;
    }


}
