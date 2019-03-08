package org.apache.spark.sql.catalyst.expressions.predicates;

import org.apache.spark.sql.catalyst.expressions.Expression;

import java.util.List;

/**
 * Created by kenya on 2019/2/22.
 */
public class EqualNullSafe extends BinaryComparison {
    public EqualNullSafe(Expression left, Expression right){
        super(left, right);
    }

    public EqualNullSafe(List<Expression> expressions){
        super(expressions.get(0),expressions.get(1));
    }
}
