package org.apache.spark.sql.catalyst.plans.logical;

/**
 * Created by kenya on 2019/1/19.
 */
public abstract class OrderPreservingUnaryNode extends UnaryNode {

    public OrderPreservingUnaryNode(LogicalPlan child){
        super(child);
    }

}
