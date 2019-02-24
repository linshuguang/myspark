package org.apache.spark.sql.catalyst.expressions.subquery;

import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.namedExpressions.ExprId;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

import java.util.List;

/**
 * Created by kenya on 2019/2/22.
 */
public class SubqueryExpression extends  PlanExpression<LogicalPlan> {
    LogicalPlan plan;
    List<Expression> children;
    ExprId exprId;

    public SubqueryExpression(
            LogicalPlan plan,
            List<Expression> children,
            ExprId exprId){
        this.plan = plan;
        this.children = children;
        this.exprId = exprId;
    }
}
