package org.apache.spark.sql.catalyst.analysis;

import lombok.Data;
import org.apache.spark.lang.PartialFunction;
import org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias;
import org.apache.spark.sql.catalyst.rules.Rule;

/**
 * Created by kenya on 2019/2/28.
 */
public class EliminateSubqueryAliases extends Rule<LogicalPlan> {

    @Override
    public LogicalPlan apply(LogicalPlan plan){
        return plan.allowInvokingTransformsInAnalyzer((Void)->{
            return plan.transformUp(new PartialFunction<>(
                    (s)->{return s instanceof SubqueryAlias;},
                    (t)->{SubqueryAlias s =(SubqueryAlias)t; return s.getChild(); }
            ));
        });
    }
}
