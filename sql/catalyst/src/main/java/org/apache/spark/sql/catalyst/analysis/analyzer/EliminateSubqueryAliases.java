package org.apache.spark.sql.catalyst.analysis.analyzer;

import org.apache.spark.lang.PartialFunction;
import org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias;
import org.apache.spark.sql.catalyst.rules.Rule;

/**
 * Created by kenya on 2019/4/7.
 */
public class EliminateSubqueryAliases  extends Rule<LogicalPlan> {
    @Override
    public LogicalPlan  apply(LogicalPlan plan) {
        return plan.allowInvokingTransformsInAnalyzer((p) -> {
            return plan.transformUp(new PartialFunction<>((s) -> {
                        if (s instanceof SubqueryAlias) {
                            return true;
                        } else {
                            return false;
                        }
                    },
                            (t) -> {
                                return ((SubqueryAlias) t).getChild();
                            }
                    )
            );
        });
    }
}
