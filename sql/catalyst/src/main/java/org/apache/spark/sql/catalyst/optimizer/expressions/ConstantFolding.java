package org.apache.spark.sql.catalyst.optimizer.expressions;

import org.apache.spark.lang.PartialFunction;
import org.apache.spark.sql.catalyst.expressions.literals.Literal;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.rules.Rule;

/**
 * Created by kenya on 2019/4/9.
 */
public class ConstantFolding extends Rule<LogicalPlan> {

    @Override
    public LogicalPlan apply(LogicalPlan plan){
        return plan.transform(
                new PartialFunction<LogicalPlan,LogicalPlan>(
                        (q)->{return q instanceof LogicalPlan;},
                        (q)->{
                            LogicalPlan p = (LogicalPlan)q;
                            p.transformDown(
                                    new PartialFunction<LogicalPlan,LogicalPlan>(
                                            (s)->{return true;},
                                            (t)->{
                                                return t;
                                            }
                                    )
                            );
                            return p;
                        }
                )
        );
//        case q: LogicalPlan => q transformExpressionsDown {
//            // Skip redundant folding of literals. This rule is technically not necessary. Placing this
//            // here avoids running the next rule for Literal values, which would create a new Literal
//            // object and running eval unnecessarily.
//            case l: Literal => l
//
//                // Fold expressions that are foldable.
//            case e if e.foldable => Literal.create(e.eval(EmptyRow), e.dataType)
//        }
    }
}
