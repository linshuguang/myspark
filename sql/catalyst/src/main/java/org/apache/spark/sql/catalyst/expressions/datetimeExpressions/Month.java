package org.apache.spark.sql.catalyst.expressions.datetimeExpressions;

import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.UnaryExpression;

/**
 * Created by kenya on 2019/2/22.
 */
public class Month extends UnaryExpression {
    public Month(Expression child){
        super(child);
    }
}