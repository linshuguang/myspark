package org.apache.spark.sql.catalyst.analysis;

import org.apache.spark.lang.PartialFunction;
import org.apache.spark.sql.catalyst.expressions.Alias;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.rules.Rule;

/**
 * Created by kenya on 2019/4/24.
 */
public class CleanupAliases extends Rule<LogicalPlan> {

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        return plan.allowInvokingTransformsInAnalyzer((Void) -> {
            return plan.transformUp(new PartialFunction<>(
                    (s) -> {
                        return true;
                    },
                    (s) -> {
                        return s.transformExpressionsDown(
                                new PartialFunction<>(
                                        (p) -> {
                                            return p instanceof Alias;
                                        },
                                        (p) -> {
                                            Alias alias = (Alias) p;
                                            return alias.getChild();
                                        }
                                )
                        );
                    }
            ));
        });
    }
}
