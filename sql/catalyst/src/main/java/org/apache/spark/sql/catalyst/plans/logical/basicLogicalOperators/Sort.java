package org.apache.spark.sql.catalyst.plans.logical.basicLogicalOperators;

import org.apache.spark.sql.catalyst.expressions.SortOrder;
import org.apache.spark.sql.catalyst.parser.ParserUtils;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.UnaryNode;

import javax.swing.text.html.parser.Parser;
import java.lang.reflect.Parameter;
import java.util.List;

/**
 * Created by kenya on 2019/2/20.
 */
public class Sort extends UnaryNode{
    List<SortOrder>order;
    boolean global;

    public Sort(List<SortOrder>order,
            boolean global,
            LogicalPlan child){
        super(child);
        this.order = order;
        this.global = global;
    }

    @Override
    public boolean equals(Object o){
        if(o instanceof Sort){
            Sort s = (Sort) o;

            return ParserUtils.equalList(order,s.order) && global==global;
        }
        return false;
    }
}
