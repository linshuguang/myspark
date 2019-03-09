package org.apache.spark.sql.catalyst.plans.logical;

import lombok.Data;
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


}
