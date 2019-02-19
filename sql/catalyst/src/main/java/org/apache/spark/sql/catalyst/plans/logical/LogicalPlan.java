package org.apache.spark.sql.catalyst.plans.logical;

import org.apache.spark.sql.catalyst.plans.QueryPlan;

/**
 * Created by kenya on 2019/1/18.
 */
public class LogicalPlan extends QueryPlan<LogicalPlan> {
    //isStreaming: Boolean = children.exists(_.isStreaming == true)

}
