package org.apache.spark.sql.catalyst.expressions.arithmetic;

import org.apache.spark.sql.catalyst.expressions.Expression;

/**
 * Created by kenya on 2019/2/22.
 */
public class Subtract extends BinaryArithmetic {
    public Subtract(Expression left, Expression right){
        super(left, right);
    }
}
