package org.apache.spark.sql.catalyst.optimizer.expressions;

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.rules.Rule;

/**
 * Created by kenya on 2019/4/15.
 */
public class BooleanSimplification  extends Rule<LogicalPlan> {

    @Override
    public LogicalPlan apply(LogicalPlan plan){
        return plan;
    }
//    def apply(plan: LogicalPlan): LogicalPlan = plan transform {
//        case q: LogicalPlan => q transformExpressionsUp {
//            case TrueLiteral And e => e
//            case e And TrueLiteral => e
//            case FalseLiteral Or e => e
//            case e Or FalseLiteral => e
//
//            case FalseLiteral And _ => FalseLiteral
//            case _ And FalseLiteral => FalseLiteral
//            case TrueLiteral Or _ => TrueLiteral
//            case _ Or TrueLiteral => TrueLiteral
//
//            case a And b if Not(a).semanticEquals(b) =>
//                If(IsNull(a), Literal.create(null, a.dataType), FalseLiteral)
//            case a And b if a.semanticEquals(Not(b)) =>
//                If(IsNull(b), Literal.create(null, b.dataType), FalseLiteral)
//
//            case a Or b if Not(a).semanticEquals(b) =>
//                If(IsNull(a), Literal.create(null, a.dataType), TrueLiteral)
//            case a Or b if a.semanticEquals(Not(b)) =>
//                If(IsNull(b), Literal.create(null, b.dataType), TrueLiteral)
//
//            case a And b if a.semanticEquals(b) => a
//            case a Or b if a.semanticEquals(b) => a
//
//                // The following optimizations are applicable only when the operands are not nullable,
//                // since the three-value logic of AND and OR are different in NULL handling.
//                // See the chart:
//                // +---------+---------+---------+---------+
//                // | operand | operand |   OR    |   AND   |
//                // +---------+---------+---------+---------+
//                // | TRUE    | TRUE    | TRUE    | TRUE    |
//                // | TRUE    | FALSE   | TRUE    | FALSE   |
//                // | FALSE   | FALSE   | FALSE   | FALSE   |
//                // | UNKNOWN | TRUE    | TRUE    | UNKNOWN |
//                // | UNKNOWN | FALSE   | UNKNOWN | FALSE   |
//                // | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN |
//                // +---------+---------+---------+---------+
//
//                // (NULL And (NULL Or FALSE)) = NULL, but (NULL And FALSE) = FALSE. Thus, a can't be nullable.
//            case a And (b Or c) if !a.nullable && Not(a).semanticEquals(b) => And(a, c)
//                // (NULL And (FALSE Or NULL)) = NULL, but (NULL And FALSE) = FALSE. Thus, a can't be nullable.
//            case a And (b Or c) if !a.nullable && Not(a).semanticEquals(c) => And(a, b)
//                // ((NULL Or FALSE) And NULL) = NULL, but (FALSE And NULL) = FALSE. Thus, c can't be nullable.
//            case (a Or b) And c if !c.nullable && a.semanticEquals(Not(c)) => And(b, c)
//                // ((FALSE Or NULL) And NULL) = NULL, but (FALSE And NULL) = FALSE. Thus, c can't be nullable.
//            case (a Or b) And c if !c.nullable && b.semanticEquals(Not(c)) => And(a, c)
//
//                // (NULL Or (NULL And TRUE)) = NULL, but (NULL Or TRUE) = TRUE. Thus, a can't be nullable.
//            case a Or (b And c) if !a.nullable && Not(a).semanticEquals(b) => Or(a, c)
//                // (NULL Or (TRUE And NULL)) = NULL, but (NULL Or TRUE) = TRUE. Thus, a can't be nullable.
//            case a Or (b And c) if !a.nullable && Not(a).semanticEquals(c) => Or(a, b)
//                // ((NULL And TRUE) Or NULL) = NULL, but (TRUE Or NULL) = TRUE. Thus, c can't be nullable.
//            case (a And b) Or c if !c.nullable && a.semanticEquals(Not(c)) => Or(b, c)
//                // ((TRUE And NULL) Or NULL) = NULL, but (TRUE Or NULL) = TRUE. Thus, c can't be nullable.
//            case (a And b) Or c if !c.nullable && b.semanticEquals(Not(c)) => Or(a, c)
//
//                // Common factor elimination for conjunction
//            case and @ (left And right) =>
//                // 1. Split left and right to get the disjunctive predicates,
//                //   i.e. lhs = (a, b), rhs = (a, c)
//                // 2. Find the common predict between lhsSet and rhsSet, i.e. common = (a)
//                // 3. Remove common predict from lhsSet and rhsSet, i.e. ldiff = (b), rdiff = (c)
//                // 4. Apply the formula, get the optimized predicate: common || (ldiff && rdiff)
//                val lhs = splitDisjunctivePredicates(left)
//                val rhs = splitDisjunctivePredicates(right)
//                val common = lhs.filter(e => rhs.exists(e.semanticEquals))
//                if (common.isEmpty) {
//                    // No common factors, return the original predicate
//                    and
//                } else {
//                    val ldiff = lhs.filterNot(e => common.exists(e.semanticEquals))
//                    val rdiff = rhs.filterNot(e => common.exists(e.semanticEquals))
//                    if (ldiff.isEmpty || rdiff.isEmpty) {
//                        // (a || b || c || ...) && (a || b) => (a || b)
//                        common.reduce(Or)
//                    } else {
//                        // (a || b || c || ...) && (a || b || d || ...) =>
//                        // ((c || ...) && (d || ...)) || a || b
//                        (common :+ And(ldiff.reduce(Or), rdiff.reduce(Or))).reduce(Or)
//                    }
//                }
//
//                // Common factor elimination for disjunction
//            case or @ (left Or right) =>
//                // 1. Split left and right to get the conjunctive predicates,
//                //   i.e.  lhs = (a, b), rhs = (a, c)
//                // 2. Find the common predict between lhsSet and rhsSet, i.e. common = (a)
//                // 3. Remove common predict from lhsSet and rhsSet, i.e. ldiff = (b), rdiff = (c)
//                // 4. Apply the formula, get the optimized predicate: common && (ldiff || rdiff)
//                val lhs = splitConjunctivePredicates(left)
//                val rhs = splitConjunctivePredicates(right)
//                val common = lhs.filter(e => rhs.exists(e.semanticEquals))
//                if (common.isEmpty) {
//                    // No common factors, return the original predicate
//                    or
//                } else {
//                    val ldiff = lhs.filterNot(e => common.exists(e.semanticEquals))
//                    val rdiff = rhs.filterNot(e => common.exists(e.semanticEquals))
//                    if (ldiff.isEmpty || rdiff.isEmpty) {
//                        // (a && b) || (a && b && c && ...) => a && b
//                        common.reduce(And)
//                    } else {
//                        // (a && b && c && ...) || (a && b && d && ...) =>
//                        // ((c && ...) || (d && ...)) && a && b
//                        (common :+ Or(ldiff.reduce(And), rdiff.reduce(And))).reduce(And)
//                    }
//                }
//
//            case Not(TrueLiteral) => FalseLiteral
//            case Not(FalseLiteral) => TrueLiteral
//
//            case Not(a GreaterThan b) => LessThanOrEqual(a, b)
//            case Not(a GreaterThanOrEqual b) => LessThan(a, b)
//
//            case Not(a LessThan b) => GreaterThanOrEqual(a, b)
//            case Not(a LessThanOrEqual b) => GreaterThan(a, b)
//
//            case Not(a Or b) => And(Not(a), Not(b))
//            case Not(a And b) => Or(Not(a), Not(b))
//
//            case Not(Not(e)) => e
//        }
//    }
}
