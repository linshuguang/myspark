package org.apache.spark.sql.catalyst.expressions.windowExpressions;

import org.apache.spark.sql.catalyst.expressions.Expression;

/**
 * Created by kenya on 2019/2/22.
 */
public class WindowExpression extends Expression{
    Expression windowFunction;
    WindowSpecDefinition windowSpec;
    public WindowExpression(Expression windowFunction,
            WindowSpecDefinition windowSpec){
        this.windowFunction = windowFunction;
        this.windowSpec = windowSpec;
    }
}
