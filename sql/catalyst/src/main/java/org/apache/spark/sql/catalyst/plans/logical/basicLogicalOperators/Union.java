package org.apache.spark.sql.catalyst.plans.logical.basicLogicalOperators;

import org.apache.spark.sql.catalyst.parser.ParserUtils;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by kenya on 2019/2/21.
 */
public class Union extends LogicalPlan {

    private List<LogicalPlan>children;
    public Union(List<LogicalPlan>children){
        this.children = children;
    }
    public Union(LogicalPlan left, LogicalPlan right){
        children = new ArrayList<>();
        children.add(left);
        children.add(right);
    }
    @Override
    protected List<LogicalPlan>children(){
        return this.children;
    }

    @Override
    public boolean equals(Object o){
        if(o instanceof Union){
            Union u = (Union)o;
            return ParserUtils.equalList(children,u.children);
        }
        return false;
    }

}
