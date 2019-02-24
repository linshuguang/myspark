package org.apache.spark.sql.catalyst.expressions.higherOrderFunctions;

import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.namedExpressions.NamedExpression;

import java.util.List;

/**
 * Created by kenya on 2019/2/22.
 */
public class LambdaFunction extends Expression{
    Expression function;
    List<NamedExpression> arguments;
    boolean hidden;
    public LambdaFunction(Expression function,
            List<NamedExpression> arguments,
            boolean hidden){
        this.function = function;
        this.arguments = arguments;
        this.hidden = hidden;
    }

    public LambdaFunction(Expression function,
                          List<NamedExpression> arguments){
        this(function,arguments,false);
    }
}
