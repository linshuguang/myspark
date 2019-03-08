package org.apache.spark.sql.catalyst.expressions;

import lombok.Data;
import org.apache.spark.sql.catalyst.expressions.namedExpressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.namedExpressions.NamedExpression;
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
}
