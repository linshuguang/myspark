package org.apache.spark.sql.catalyst.expressions.subquery;

import lombok.experimental.NonFinal;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.namedExpressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.namedExpressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by kenya on 2019/2/22.
 */
public class ListQuery extends SubqueryExpression {
    List<Attribute>childOutputs;

    public ListQuery(LogicalPlan plan,
            List<Expression> children,
            ExprId exprId,
            List<Attribute>childOutputs){
        super(plan, children,exprId);
        this.childOutputs = childOutputs;
    }
    public ListQuery(LogicalPlan plan){
        this(plan,new ArrayList<>(), NamedExpression.newExprId(),new ArrayList<>());
    }

    @Override
    public ListQuery clone(){
        return new ListQuery(getPlan(),getChildren(),getExprId(), childOutputs);
    }

}
