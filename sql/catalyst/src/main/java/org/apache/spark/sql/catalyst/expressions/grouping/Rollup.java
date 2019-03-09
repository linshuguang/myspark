package org.apache.spark.sql.catalyst.expressions.grouping;

import org.apache.spark.sql.catalyst.expressions.Expression;

import java.util.List;

/**
 * Created by kenya on 2019/2/15.
 */
public class Rollup extends GroupingSet {
    List<Expression> groupByExprs;

    public Rollup(List<Expression>groupByExprs){
        this.groupByExprs = groupByExprs;
    }

    @Override
    List<Expression> groupByExprs(){
        return groupByExprs;
    }
}
