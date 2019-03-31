package org.apache.spark.sql.catalyst.expressions.arithmetic;

import org.apache.spark.sql.catalyst.expressions.Expression;

/**
 * Created by kenya on 2019/2/22.
 */
public class Divide extends BinaryArithmetic {
    public Divide(Expression left, Expression right) {
        super(left, right);
    }

}
