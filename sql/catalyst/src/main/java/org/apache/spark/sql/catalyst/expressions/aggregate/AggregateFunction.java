package org.apache.spark.sql.catalyst.expressions.aggregate;

import org.apache.spark.sql.catalyst.expressions.Expression;

/**
 * Created by kenya on 2019/2/22.
 */
public class AggregateFunction extends Expression {
    public AggregateExpression toAggregateExpression(){
        return toAggregateExpression(false);
    }

    public AggregateExpression toAggregateExpression(boolean isDistinct){
        return new AggregateExpression(this, new Complete(),isDistinct);
    }
}
