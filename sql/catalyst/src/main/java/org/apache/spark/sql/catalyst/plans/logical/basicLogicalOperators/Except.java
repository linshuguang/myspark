package org.apache.spark.sql.catalyst.plans.logical.basicLogicalOperators;

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * Created by kenya on 2019/2/21.
 */
public class Except extends SetOperation {
    boolean isAll;
    public Except(LogicalPlan left, LogicalPlan right, boolean isAll){
        super(left, right);
        this.isAll = isAll;
    }

    @Override
    public boolean equals(Object o){
        if(o instanceof Except){
            return super.equals(o);
        }
        return false;
    }
}
