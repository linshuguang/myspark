package org.apache.spark.sql.catalyst.plans.logical;

import lombok.Data;
import org.apache.spark.sql.catalyst.parser.ParserUtils;

import java.util.Arrays;
import java.util.List;

/**
 * Created by kenya on 2019/1/21.
 */
@Data
public abstract class BinaryNode extends LogicalPlan {
    private LogicalPlan left;
    private LogicalPlan right;

    public BinaryNode(LogicalPlan left, LogicalPlan right){
        this.left = left;
        this.right = right;
    }

    @Override
    protected final List<LogicalPlan> children(){
        LogicalPlan[] logicalPlans = new LogicalPlan[]{left,right};
        return Arrays.asList(logicalPlans);
    }

    @Override
    public boolean equals(Object o){
        if(o instanceof BinaryNode){
            BinaryNode b = (BinaryNode)o;
            return ParserUtils.equals(left,b.left) && ParserUtils.equals(right,b.right);
        }
        return false;
    }


}
