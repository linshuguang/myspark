package org.apache.spark.sql.catalyst.expressions.arithmetic;

import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.UnaryExpression;

/**
 * Created by kenya on 2019/2/22.
 */
public class UnaryMinus extends UnaryExpression {
    public UnaryMinus(Expression child){
        super(child);
    }
}
