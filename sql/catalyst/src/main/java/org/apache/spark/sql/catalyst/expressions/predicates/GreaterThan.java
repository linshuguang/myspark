package org.apache.spark.sql.catalyst.expressions.predicates;

import org.apache.spark.sql.catalyst.expressions.Expression;

/**
 * Created by kenya on 2019/2/22.
 */
public class GreaterThan extends BinaryComparison {
    public GreaterThan(Expression left, Expression right){
        super(left, right);
    }

    @Override
    public boolean equals(Object o){
        if(o instanceof GreaterThan){
            return super.equals(o);
        }
        return false;
    }
}
