package org.apache.spark.sql.catalyst.plans.logical.basicLogicalOperators;

import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.grouping.GroupingSet;
import org.apache.spark.sql.catalyst.expressions.namedExpressions.NamedExpression;
import org.apache.spark.sql.catalyst.parser.ParserUtils;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.UnaryNode;

import java.util.List;

/**
 * Created by kenya on 2019/2/15.
 */
public class GroupingSets extends UnaryNode {
    List<List<Expression>>selectedGroupByExprs;
    List<Expression>groupByExprs;
    List<NamedExpression>aggregations;

    public GroupingSets(List<List<Expression>>selectedGroupByExprs,
            List<Expression>groupByExprs,
            LogicalPlan child,
            List<NamedExpression>aggregations){
        super(child);
        this.selectedGroupByExprs = selectedGroupByExprs;
        this.groupByExprs = groupByExprs;
        this.aggregations = aggregations;
    }

    @Override
    public boolean equals(Object o){
        if(o instanceof GroupingSets){
            GroupingSets g  = (GroupingSets)o;
            if(!ParserUtils.equalList(selectedGroupByExprs,g.selectedGroupByExprs)){
                return false;
            }
            if(!ParserUtils.equalList(groupByExprs,g.groupByExprs)){
                return false;
            }
            if(!ParserUtils.equalList(aggregations,g.aggregations)){
                return false;
            }
            return super.equals(o);
        }
        return false;
    }
}
