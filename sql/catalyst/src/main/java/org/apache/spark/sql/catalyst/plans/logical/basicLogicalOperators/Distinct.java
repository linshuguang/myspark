package org.apache.spark.sql.catalyst.plans.logical.basicLogicalOperators;

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.UnaryNode;

/**
 * Created by kenya on 2019/2/18.
 */
public class Distinct extends UnaryNode {
    LogicalPlan child;

    public Distinct(LogicalPlan child){
        this.child = child;
    }
}
