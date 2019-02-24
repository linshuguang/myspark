package org.apache.spark.sql.catalyst.expressions.aggregate;

import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.namedExpressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.namedExpressions.NamedExpression;

/**
 * Created by kenya on 2019/2/22.
 */
public class AggregateExpression extends Expression {
    AggregateFunction aggregateFunction;
    AggregateMode mode;
    boolean isDistinct;
    ExprId resultId;

    public AggregateExpression(AggregateFunction aggregateFunction,
            AggregateMode mode,
            boolean isDistinct,
            ExprId resultId){
        this.aggregateFunction = aggregateFunction;
        this.mode = mode;
        this.isDistinct = isDistinct;
        this.resultId = resultId;
    }

    public AggregateExpression(AggregateFunction aggregateFunction,
                               AggregateMode mode,
                               boolean isDistinct){
        this(aggregateFunction,mode, isDistinct, NamedExpression.newExprId());
    }
}
