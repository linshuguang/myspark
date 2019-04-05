package org.apache.spark.sql.catalyst.expressions.predicates;

import org.apache.spark.sql.catalyst.expressions.Expression;

/**
 * Created by kenya on 2019/2/22.
 */
public class GreaterThanOrEqual extends BinaryComparison {
    public GreaterThanOrEqual(Expression left, Expression right){
        super(left, right);
    }

    @Override
    public boolean equals(Object o){
        if(o instanceof GreaterThanOrEqual){
            return super.equals(o);
        }
        return false;
    }
}
