package org.apache.spark.sql.catalyst.expressions;

/**
 * Created by kenya on 2019/2/21.
 */
public class And extends BinaryOperator {
    public And(Expression left, Expression right){
        super(left, right);
    }

}
