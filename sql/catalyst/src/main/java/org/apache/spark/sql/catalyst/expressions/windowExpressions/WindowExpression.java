package org.apache.spark.sql.catalyst.expressions.windowExpressions;

import org.apache.spark.sql.catalyst.expressions.Expression;

import java.util.Arrays;
import java.util.List;

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

    @Override
    protected List<Expression> children(){
        return Arrays.asList(windowFunction,windowSpec);
    }
}
