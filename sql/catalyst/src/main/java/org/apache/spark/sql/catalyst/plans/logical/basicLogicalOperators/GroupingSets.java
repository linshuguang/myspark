package org.apache.spark.sql.catalyst.plans.logical.basicLogicalOperators;

import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.namedExpressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.UnaryNode;

import java.util.List;

/**
 * Created by kenya on 2019/2/15.
 */
public class GroupingSets extends UnaryNode {
    List<List<Expression>>selectedGroupByExprs;
    List<Expression>groupByExprs;
    LogicalPlan child;
    List<NamedExpression>aggregations;

    public GroupingSets(List<List<Expression>>selectedGroupByExprs,
            List<Expression>groupByExprs,
            LogicalPlan child,
            List<NamedExpression>aggregations){
        this.selectedGroupByExprs = selectedGroupByExprs;
        this.groupByExprs = groupByExprs;
        this.child = child;
        this.aggregations = aggregations;
    }
}
