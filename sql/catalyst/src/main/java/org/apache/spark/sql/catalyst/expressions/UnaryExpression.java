package org.apache.spark.sql.catalyst.expressions;

import java.util.Arrays;
import java.util.List;

/**
 * Created by kenya on 2019/2/14.
 */
public abstract class UnaryExpression extends Expression {
    Expression child;
    public UnaryExpression(Expression child){
        this.child = child;
    }

    @Override
    protected final List<Expression>children(){
        return Arrays.asList(child);
    }
}
