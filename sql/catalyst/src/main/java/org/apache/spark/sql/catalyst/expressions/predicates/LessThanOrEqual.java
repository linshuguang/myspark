package org.apache.spark.sql.catalyst.expressions.predicates;

import org.apache.spark.sql.catalyst.expressions.Expression;

/**
 * Created by kenya on 2019/2/22.
 */
public class LessThanOrEqual extends BinaryComparison {
    public LessThanOrEqual(Expression left, Expression right){
        super(left, right);
    }
    @Override
    public boolean equals(Object o){
        if(o instanceof LessThanOrEqual){
            return super.equals(o);
        }
        return false;
    }
}
