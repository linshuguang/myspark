package org.apache.spark.sql.catalyst.plans.logical.basicLogicalOperators;

import org.apache.spark.sql.catalyst.expressions.SortOrder;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.UnaryNode;

import java.util.List;

/**
 * Created by kenya on 2019/2/20.
 */
public class Sort extends UnaryNode{
    List<SortOrder>order;
    boolean global;

    public Sort(List<SortOrder>order,
            boolean global,
            LogicalPlan child){
        super(child);
        this.order = order;
        this.global = global;
    }
}
