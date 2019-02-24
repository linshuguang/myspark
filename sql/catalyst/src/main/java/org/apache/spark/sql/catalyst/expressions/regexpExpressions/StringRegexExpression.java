package org.apache.spark.sql.catalyst.expressions.regexpExpressions;

import org.apache.spark.sql.catalyst.expressions.BinaryExpression;
import org.apache.spark.sql.catalyst.expressions.Expression;

/**
 * Created by kenya on 2019/2/22.
 */
public class StringRegexExpression extends BinaryExpression {
    public StringRegexExpression(Expression left, Expression right){
        super(left, right);
    }
}
