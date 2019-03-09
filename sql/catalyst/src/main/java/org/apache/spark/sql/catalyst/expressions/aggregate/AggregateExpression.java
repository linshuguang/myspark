package org.apache.spark.sql.catalyst.expressions.aggregate;

import lombok.Data;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.namedExpressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.namedExpressions.NamedExpression;

import java.util.Arrays;
import java.util.List;

/**
 * Created by kenya on 2019/2/22.
 */
@Data
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

    @Override
    public AggregateExpression clone(){
        return new AggregateExpression(aggregateFunction, mode, isDistinct, resultId);
    }

    @Override
    protected List<Expression> children() {
        return Arrays.asList(aggregateFunction);
    }

}
