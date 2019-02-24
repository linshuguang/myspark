package org.apache.spark.sql.catalyst.expressions.subquery;

import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.QueryPlan;

/**
 * Created by kenya on 2019/2/22.
 */
public class PlanExpression <T extends QueryPlan> extends Expression{
     T plan ;
}
