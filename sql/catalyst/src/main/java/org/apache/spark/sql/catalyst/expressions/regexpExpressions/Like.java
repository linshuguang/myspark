package org.apache.spark.sql.catalyst.expressions.regexpExpressions;

import org.apache.spark.sql.catalyst.expressions.Expression;

/**
 * Created by kenya on 2019/2/22.
 */
public class Like extends StringRegexExpression{
    public Like(Expression left, Expression right){
        super(left, right);
    }
}
