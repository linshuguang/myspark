package org.apache.spark.sql.catalyst.expressions.subquery;

import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.namedExpressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.namedExpressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by kenya on 2019/2/22.
 */
public class ScalarSubquery extends SubqueryExpression {
    public ScalarSubquery(
            LogicalPlan plan,
            List<Expression> children,
            ExprId exprId){
        super(plan, children, exprId);
    }
    public ScalarSubquery(LogicalPlan plan){
                this(plan, new ArrayList<>(), NamedExpression.newExprId());
    }


    @Override
    public ScalarSubquery clone(){
        return new ScalarSubquery(getPlan(),getChildren(),getExprId());
    }

}
