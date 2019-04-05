package org.apache.spark.sql.catalyst.expressions.predicates;

import org.apache.spark.sql.catalyst.expressions.BinaryOperator;
import org.apache.spark.sql.catalyst.expressions.Expression;

import java.util.List;

/**
 * Created by kenya on 2019/2/21.
 */
public class And extends BinaryOperator{
    public And(Expression left, Expression right){
        super(left, right);
    }

    public And(List<Expression> expressionList ){
        super(expressionList.get(0), expressionList.get(1));
    }

    @Override
    public boolean equals(Object o){
        if(o instanceof And){
            return super.equals(o);
        }
        return false;
    }

}
