package org.apache.spark.sql.catalyst.plans.logical;

/**
 * Created by kenya on 2019/1/21.
 */
public class BinaryNode extends LogicalPlan {
    LogicalPlan left;
    LogicalPlan right;

    public BinaryNode(LogicalPlan left, LogicalPlan right){
        this.left = left;
        this.right = right;
    }

}
