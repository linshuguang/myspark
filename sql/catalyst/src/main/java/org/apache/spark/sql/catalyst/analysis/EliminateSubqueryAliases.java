package org.apache.spark.sql.catalyst.analysis;

import lombok.Data;
import org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.trees.Rule;

/**
 * Created by kenya on 2019/2/28.
 */
public class EliminateSubqueryAliases extends Rule<LogicalPlan> {
    @Override
    public LogicalPlan apply(LogicalPlan plan){
//
//        AnalysisHelper.allowInvokingTransformsInAnalyzer(()->{
//        }
//    }) {
//        plan transformUp {
//            case SubqueryAlias(_, child) => child
//        }
        return plan;
    }
}
