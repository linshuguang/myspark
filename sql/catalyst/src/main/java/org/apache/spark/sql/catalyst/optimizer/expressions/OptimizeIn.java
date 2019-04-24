package org.apache.spark.sql.catalyst.optimizer.expressions;

import org.apache.spark.lang.PartialFunction;
import org.apache.spark.sql.catalyst.expressions.predicates.In;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.rules.Rule;

/**
 * Created by kenya on 2019/4/9.
 */
public class OptimizeIn extends Rule<LogicalPlan> {

    @Override
    public LogicalPlan apply(LogicalPlan plan){
        return plan.transform(new PartialFunction<LogicalPlan,LogicalPlan>(
                (p)->{return p instanceof LogicalPlan;},
                (q)->{
                    LogicalPlan p = (LogicalPlan)q;
//
//                    if(p instanceof In){
//
//                    }

                    return p;
                    }
                )
        );
//        case q: LogicalPlan => q transformExpressionsDown {
//            case In(v, list) if list.isEmpty =>
//                // When v is not nullable, the following expression will be optimized
//                // to FalseLiteral which is tested in OptimizeInSuite.scala
//                If(IsNotNull(v), FalseLiteral, Literal(null, BooleanType))
//            case expr @ In(v, list) if expr.inSetConvertible =>
//                val newList = ExpressionSet(list).toSeq
//                if (newList.length == 1
//                        // TODO: `EqualTo` for structural types are not working. Until SPARK-24443 is addressed,
//                        // TODO: we exclude them in this rule.
//                        && !v.isInstanceOf[CreateNamedStructLike]
//                        && !newList.head.isInstanceOf[CreateNamedStructLike]) {
//                    EqualTo(v, newList.head)
//                } else if (newList.length > SQLConf.get.optimizerInSetConversionThreshold) {
//                    val hSet = newList.map(e => e.eval(EmptyRow))
//                    InSet(v, HashSet() ++ hSet)
//                } else if (newList.length < list.length) {
//                    expr.copy(list = newList)
//                } else { // newList.length == list.length && newList.length > 1
//                    expr
//                }
        }
}
