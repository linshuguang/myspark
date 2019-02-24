package org.apache.spark.sql.catalyst.expressions.bitwiseExpressions;

import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.arithmetic.BinaryArithmetic;

/**
 * Created by kenya on 2019/2/22.
 */
public class BitwiseXor extends BinaryArithmetic {
    public BitwiseXor(Expression left, Expression right){
        super(left, right);
    }
}
