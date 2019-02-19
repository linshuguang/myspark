package org.apache.spark.sql.catalyst.analysis.unresolved;

import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.namedExpressions.NamedExpression;

import java.util.function.Function;

/**
 * Created by kenya on 2019/2/15.
 */
public class UnresolvedAlias extends NamedExpression {
    Expression child;
    Function<Expression, String>aliasFunc;

    public UnresolvedAlias(Expression child,Function<Expression, String>aliasFunc){
        this.child = child;
        this.aliasFunc = aliasFunc;
    }
    public UnresolvedAlias(Expression child){
        this(child, null);
    }
}
