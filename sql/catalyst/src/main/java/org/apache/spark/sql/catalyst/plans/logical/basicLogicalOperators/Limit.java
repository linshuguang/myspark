package org.apache.spark.sql.catalyst.plans.logical.basicLogicalOperators;

import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * Created by kenya on 2019/2/21.
 */
public class Limit {
    public static LogicalPlan build(Expression limitExpr, LogicalPlan child){
        return new GlobalLimit(limitExpr, child);
    }
}
