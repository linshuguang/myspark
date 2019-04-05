package org.apache.spark.sql.catalyst.expressions;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.catalyst.expressions.namedExpressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.namedExpressions.NamedExpression;
import org.apache.spark.sql.catalyst.parser.ParserUtils;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Metadata;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by kenya on 2019/2/14.
 */
@Data
public class AttributeReference extends Attribute {
    String name;
    DataType dataType;
    boolean nullable;
    Metadata metadata ;

    ExprId exprId;
    List<String> qualifier;

    public AttributeReference(String name, DataType dataType, boolean nullable, Metadata metadata){
        this.name = name;
        this.dataType = dataType;
        this.nullable = nullable;
        this.metadata = metadata;
        this.exprId = NamedExpression.newExprId();
        this.qualifier = new ArrayList<>();
    }

    public AttributeReference(String name, DataType dataType, boolean nullable){
        this(name, dataType, nullable, new Metadata());
    }

    public AttributeReference(String name, DataType dataType){
        this(name, dataType, true, new Metadata());
    }

    @Override
    public boolean equals(Object o){
        if(o instanceof AttributeReference){
            AttributeReference a= (AttributeReference)o;
            if(!StringUtils.equals(name,a.name)){
                return false;
            }
            if(!ParserUtils.equals(dataType,a.dataType)){
                return false;
            }
            if(nullable!=a.nullable){
                return false;
            }
            if(!ParserUtils.equals(metadata,a.metadata)){
                return false;
            }
            if(!ParserUtils.equals(exprId,a.exprId)){
                return false;
            }
            if(!ParserUtils.equalList(qualifier,a.qualifier)){
                return false;
            }
            return true;
        }
        return true;
    }
}
