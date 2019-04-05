package org.apache.spark.sql.catalyst.expressions.predicates;

import org.apache.spark.sql.catalyst.expressions.Expression;

import java.util.List;

/**
 * Created by kenya on 2019/2/22.
 */
public class EqualTo extends BinaryComparison {
    Expression left;
    Expression right;
    public EqualTo(Expression left,Expression right){
        super(left, right);
    }
    public EqualTo(List<Expression> expressionList ){
        super(expressionList.get(0), expressionList.get(1));
    }

    @Override
    public boolean equals(Object o){
        if(o instanceof EqualTo){
            return super.equals(o);
        }
        return false;
    }

}
