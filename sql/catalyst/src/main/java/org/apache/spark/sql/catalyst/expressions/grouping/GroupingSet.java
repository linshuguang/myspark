package org.apache.spark.sql.catalyst.expressions.grouping;

import org.apache.spark.sql.catalyst.expressions.Expression;

import java.util.List;

/**
 * Created by kenya on 2019/2/15.
 */
public abstract class GroupingSet extends Expression {

    abstract List<Expression>groupByExprs();

    @Override
    protected List<Expression> children(){
        return groupByExprs();
    }
}
