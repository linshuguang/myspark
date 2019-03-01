package org.apache.spark.sql.catalyst.expressions.arithmetic;

import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.UnaryExpression;

/**
 * Created by kenya on 2019/2/27.
 */
public class Abs extends UnaryExpression {
    public Abs(Expression child){
        super(child);
    }
}
