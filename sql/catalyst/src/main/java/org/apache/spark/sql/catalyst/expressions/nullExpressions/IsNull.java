package org.apache.spark.sql.catalyst.expressions.nullExpressions;

import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.UnaryExpression;

/**
 * Created by kenya on 2019/2/22.
 */
public class IsNull extends UnaryExpression {
    public IsNull(Expression child){
        super(child);
    }
}
