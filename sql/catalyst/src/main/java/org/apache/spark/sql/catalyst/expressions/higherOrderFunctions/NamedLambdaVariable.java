package org.apache.spark.sql.catalyst.expressions.higherOrderFunctions;

import lombok.Data;
import org.apache.spark.sql.catalyst.expressions.LeafExpression;
import org.apache.spark.sql.catalyst.expressions.namedExpressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.namedExpressions.NamedExpression;
import org.apache.spark.sql.types.DataType;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by kenya on 2019/3/8.
 */
@Data
public class NamedLambdaVariable extends LeafExpression {
    String name;
    DataType dataType;
    boolean nullable;
    ExprId exprId;
    AtomicReference value;

    public NamedLambdaVariable(String name,
            DataType dataType,
            boolean nullable,
            ExprId exprId,
            AtomicReference value){
        this.name = name;
        this.dataType = dataType;
        this.nullable = nullable;
        this.exprId = exprId;
        this.value = value;
    }

    public NamedLambdaVariable(String name,
                               DataType dataType,
                               boolean nullable
     ){
        this(name,dataType,nullable, NamedExpression.newExprId(),new AtomicReference());
    }

    @Override
    public NamedLambdaVariable clone(){
        return new NamedLambdaVariable(name, dataType, nullable, exprId, value);
    }
}
