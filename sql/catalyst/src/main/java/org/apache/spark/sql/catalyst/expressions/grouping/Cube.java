package org.apache.spark.sql.catalyst.expressions.grouping;


import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.parser.ParserUtils;

import java.util.List;

/**
 * Created by kenya on 2019/2/15.
 */
public class Cube extends GroupingSet {
    List<Expression>groupByExprs;

    public Cube(List<Expression>groupByExprs){
        this.groupByExprs = groupByExprs;
    }

    @Override
    List<Expression> groupByExprs(){
        return groupByExprs;
    }

    @Override
    public boolean equals(Object o){
        if(o instanceof Cube){
            Cube cube = (Cube)o;
            return ParserUtils.equalList(groupByExprs,cube.groupByExprs);
        }
        return false;
    }
}
