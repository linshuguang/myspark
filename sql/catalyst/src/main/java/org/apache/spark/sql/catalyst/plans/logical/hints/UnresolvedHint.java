package org.apache.spark.sql.catalyst.plans.logical.hints;

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.UnaryNode;

import java.util.List;

/**
 * Created by kenya on 2019/2/20.
 */
public class UnresolvedHint extends UnaryNode {
    String name;
    List<Object>  parameters;
    LogicalPlan child;
    public UnresolvedHint(String name,
            List<Object>  parameters,
            LogicalPlan child){
        this.name = name;
        this.parameters = parameters;
        this.child = child;
    }

}
