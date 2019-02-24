package org.apache.spark.sql.catalyst.expressions.predicates;

import org.apache.spark.sql.catalyst.expressions.Expression;

/**
 * Created by kenya on 2019/2/22.
 */
public class LessThan extends BinaryComparison {
    public LessThan(Expression left, Expression right){
        super(left, right);
    }
}
