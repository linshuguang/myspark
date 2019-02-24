package org.apache.spark.sql.catalyst.plans.logical.basicLogicalOperators;

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by kenya on 2019/2/21.
 */
public class Union extends LogicalPlan {

    List<LogicalPlan>children;
    public Union(List<LogicalPlan>children){
        this.children = children;
    }
    public Union(LogicalPlan left, LogicalPlan right){
        children = new ArrayList<>();
        children.add(left);
        children.add(right);
    }
}
