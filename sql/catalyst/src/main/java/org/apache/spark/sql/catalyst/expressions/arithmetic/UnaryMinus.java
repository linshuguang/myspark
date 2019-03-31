package org.apache.spark.sql.catalyst.expressions.arithmetic;

import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.UnaryExpression;

/**
 * Created by kenya on 2019/2/22.
 */
public class UnaryMinus extends UnaryExpression {
    public UnaryMinus(Expression child){
        super(child);
    }
    @Override
    public boolean equals(Object o){
        if(o instanceof UnaryMinus){
            return super.equals(o);
        }
        return false;
    }
}
