package org.apache.spark.sql.catalyst.expressions.bitwiseExpressions;

import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.arithmetic.BinaryArithmetic;

/**
 * Created by kenya on 2019/2/22.
 */
public class BitwiseAnd extends BinaryArithmetic {
    public BitwiseAnd(Expression left, Expression right){
        super(left, right);
    }
}
