package org.apache.spark.sql.catalyst.plans.logical.basicLogicalOperators;

import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.OrderPreservingUnaryNode;

/**
 * Created by kenya on 2019/2/14.
 */
public class Filter extends OrderPreservingUnaryNode {
    Expression condition;
    LogicalPlan child;

    public Filter(Expression condition, LogicalPlan child){
        this.condition = condition;
        this.child = child;
    }

}
