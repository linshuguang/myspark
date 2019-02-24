package org.apache.spark.sql.catalyst.plans.logical.basicLogicalOperators;

import org.apache.spark.sql.catalyst.plans.logical.BinaryNode;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * Created by kenya on 2019/2/21.
 */
public class SetOperation extends BinaryNode {
    public SetOperation(LogicalPlan left, LogicalPlan right){
        super(left, right);
    }
}
