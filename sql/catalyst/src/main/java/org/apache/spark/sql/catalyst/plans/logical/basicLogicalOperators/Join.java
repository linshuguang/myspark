package org.apache.spark.sql.catalyst.plans.logical.basicLogicalOperators;

import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.joinTypes.JoinType;
import org.apache.spark.sql.catalyst.plans.logical.BinaryNode;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * Created by kenya on 2019/1/21.
 */
public class Join extends BinaryNode {
    JoinType joinType;
    Expression condition;

    public Join(LogicalPlan left, LogicalPlan right, JoinType joinType, Expression condition){
        super(left, right);
        this.joinType = joinType;
        this.condition = condition;
    }


}
