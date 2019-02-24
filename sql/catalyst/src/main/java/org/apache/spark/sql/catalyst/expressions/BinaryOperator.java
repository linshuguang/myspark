package org.apache.spark.sql.catalyst.expressions;

/**
 * Created by kenya on 2019/2/21.
 */
public abstract class BinaryOperator extends BinaryExpression {
    public BinaryOperator(Expression left, Expression right){
        super(left, right);
    }
}
