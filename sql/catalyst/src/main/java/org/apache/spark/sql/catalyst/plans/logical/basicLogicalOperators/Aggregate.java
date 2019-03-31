package org.apache.spark.sql.catalyst.plans.logical.basicLogicalOperators;

import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.namedExpressions.NamedExpression;
import org.apache.spark.sql.catalyst.parser.ParserUtils;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.UnaryNode;

import java.util.List;

/**
 * Created by kenya on 2019/2/15.
 */
public class Aggregate extends UnaryNode {
    List<Expression>groupingExpressions;
    List<NamedExpression>aggregateExpressions;

    public Aggregate(List<Expression>groupingExpressions,
            List<NamedExpression>aggregateExpressions,
            LogicalPlan child){
        super(child);
        this.groupingExpressions = groupingExpressions;
        this.aggregateExpressions = aggregateExpressions;
    }

    @Override
    public boolean equals(Object o){
        if(o instanceof Aggregate){
            Aggregate a = (Aggregate)o;
            return ParserUtils.equalList(groupingExpressions,a.groupingExpressions) && ParserUtils.equalList(aggregateExpressions,a.aggregateExpressions);
        }
        return false;
    }

}
