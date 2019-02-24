package org.apache.spark.sql.catalyst.analysis.unresolved;

import org.apache.spark.sql.catalyst.expressions.BinaryExpression;
import org.apache.spark.sql.catalyst.expressions.Expression;

/**
 * Created by kenya on 2019/2/22.
 */
public class UnresolvedExtractValue extends BinaryExpression{
    public UnresolvedExtractValue(Expression child, Expression extraction){
        super(child, extraction);
    }
}
