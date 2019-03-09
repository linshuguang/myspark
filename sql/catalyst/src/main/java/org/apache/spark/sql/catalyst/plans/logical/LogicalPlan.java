package org.apache.spark.sql.catalyst.plans.logical;

import org.apache.spark.sql.catalyst.plans.QueryPlan;

import java.util.function.Function;

/**
 * Created by kenya on 2019/1/18.
 */
public abstract class LogicalPlan extends QueryPlan<LogicalPlan> {
    //isStreaming: Boolean = children.exists(_.isStreaming == true)


    @Override
    public String toString(){

        return this.getClass().getSimpleName();
    }
}
