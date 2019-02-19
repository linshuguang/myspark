package org.apache.spark.sql.catalyst.plans.logical.basicLogicalOperators;

import org.apache.spark.sql.catalyst.expressions.namedExpressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.OrderPreservingUnaryNode;

import java.util.List;

/**
 * Created by kenya on 2019/2/15.
 */
public class Project extends OrderPreservingUnaryNode {
    List<NamedExpression> projectList;
    LogicalPlan child;
    public Project(List<NamedExpression> projectList, LogicalPlan child){
        this.projectList = projectList;
        this.child = child;
    }
}
