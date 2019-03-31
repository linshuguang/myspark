package org.apache.spark.sql.catalyst.plans.logical;

import lombok.Data;
import org.apache.spark.sql.catalyst.parser.ParserUtils;
import sun.rmi.runtime.Log;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by kenya on 2019/1/19.
 */
@Data
public abstract class UnaryNode extends LogicalPlan {
    private LogicalPlan child;

    public UnaryNode(LogicalPlan child){
        this.child = child;
    }

    @Override
    protected final List<LogicalPlan> children(){
        List<LogicalPlan>c= new ArrayList<>();
        c.add(child);
        return c;
    }


    @Override
    public boolean equals(Object o){
        if(o instanceof UnaryNode){
            UnaryNode u = (UnaryNode)o;
            return ParserUtils.equals(child,u.child);
        }
        return false;
    }


}
