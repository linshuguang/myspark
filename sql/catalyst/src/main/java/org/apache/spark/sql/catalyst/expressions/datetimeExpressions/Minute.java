package org.apache.spark.sql.catalyst.expressions.datetimeExpressions;

import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.UnaryExpression;

/**
 * Created by kenya on 2019/2/22.
 */
public class Minute  extends UnaryExpression {
    public Minute(Expression child){
        super(child);
    }
}