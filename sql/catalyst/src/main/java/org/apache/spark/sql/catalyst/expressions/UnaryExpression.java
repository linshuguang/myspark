package org.apache.spark.sql.catalyst.expressions;

/**
 * Created by kenya on 2019/2/14.
 */
public class UnaryExpression extends Expression {
    Expression child;
    public UnaryExpression(Expression child){
        this.child = child;
    }
}
