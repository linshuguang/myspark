package org.apache.spark.sql.catalyst.expressions;

/**
 * Created by kenya on 2019/2/21.
 */
public class BinaryExpression extends Expression  {
    Expression left;
    Expression right;
    public BinaryExpression(Expression left, Expression right){
        this.left = left;
        this.right = right;
    }

}
