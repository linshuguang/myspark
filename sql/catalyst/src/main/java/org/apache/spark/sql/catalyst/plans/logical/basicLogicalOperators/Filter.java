package org.apache.spark.sql.catalyst.plans.logical.basicLogicalOperators;

import lombok.Data;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.OrderPreservingUnaryNode;

/**
 * Created by kenya on 2019/2/14.
 */
@Data
public class Filter extends OrderPreservingUnaryNode {
    Expression condition;

    public Filter(Expression condition, LogicalPlan child){
        super(child);
        this.condition = condition;
    }



}
