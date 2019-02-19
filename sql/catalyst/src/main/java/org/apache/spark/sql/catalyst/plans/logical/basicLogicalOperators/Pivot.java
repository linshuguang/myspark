package org.apache.spark.sql.catalyst.plans.logical.basicLogicalOperators;

import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.namedExpressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.UnaryNode;

import java.util.List;

/**
 * Created by kenya on 2019/2/14.
 */
public class Pivot extends UnaryNode {
    List<NamedExpression>groupByExprsOpt;
    Expression pivotColumn;
    List<Expression> pivotValues;
    List<Expression> aggregates;
    LogicalPlan child;

    public Pivot(List<NamedExpression>groupByExprsOpt,
                 Expression pivotColumn,
                 List<Expression> pivotValues,
                 List<Expression> aggregates,
                 LogicalPlan child){
        this.groupByExprsOpt = groupByExprsOpt;
        this.pivotColumn = pivotColumn;
        this.pivotValues = pivotValues;
        this.aggregates = aggregates;
        this.child = child;
    }

}
