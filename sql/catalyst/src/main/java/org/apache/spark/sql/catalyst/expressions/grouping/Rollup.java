package org.apache.spark.sql.catalyst.expressions.grouping;

import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.parser.ParserUtils;

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

    @Override
    public boolean equals(Object o){
        if(o instanceof Rollup){
            Rollup rollup = (Rollup)o;
            return ParserUtils.equalList(groupByExprs,rollup.groupByExprs);
        }
        return false;
    }
}
