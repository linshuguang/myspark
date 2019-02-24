package org.apache.spark.sql.catalyst.analysis.unresolved;

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.UnaryNode;

import java.util.List;

/**
 * Created by kenya on 2019/2/21.
 */
public class UnresolvedSubqueryColumnAliases extends UnaryNode {
    List<String > outputColumnNames;
    LogicalPlan child;

    public UnresolvedSubqueryColumnAliases(List<String > outputColumnNames,
            LogicalPlan child){
        this.outputColumnNames = outputColumnNames;
        this.child = child;
    }
}
