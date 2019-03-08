package org.apache.spark.sql.catalyst.plans;

import org.apache.spark.SparkFunSuite;
import org.apache.spark.lang.PartialFunction;
import org.apache.spark.sql.catalyst.analysis.CheckAnalysis;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression;
import org.apache.spark.sql.catalyst.expressions.higherOrderFunctions.NamedLambdaVariable;
import org.apache.spark.sql.catalyst.expressions.namedExpressions.Alias;
import org.apache.spark.sql.catalyst.expressions.namedExpressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.predicates.And;
import org.apache.spark.sql.catalyst.expressions.predicates.EqualNullSafe;
import org.apache.spark.sql.catalyst.expressions.predicates.EqualTo;
import org.apache.spark.sql.catalyst.expressions.subquery.Exists;
import org.apache.spark.sql.catalyst.expressions.subquery.ListQuery;
import org.apache.spark.sql.catalyst.expressions.subquery.ScalarSubquery;
import org.apache.spark.sql.catalyst.parser.ParserUtils;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.basicLogicalOperators.Filter;
import org.apache.spark.sql.catalyst.plans.logical.basicLogicalOperators.Join;
import org.apache.spark.sql.catalyst.plans.logical.basicLogicalOperators.OneRowRelation;
import org.apache.spark.sql.catalyst.plans.logical.basicLogicalOperators.Sample;
import org.apache.spark.sql.catalyst.trees.TreeNode;
import sun.rmi.runtime.Log;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.spark.sql.catalyst.expressions.predicates.PredicateHelper.*;

/**
 * Created by kenya on 2019/2/26.
 */
public class PlanTest extends SparkFunSuite {

    protected void compareExpressions(Expression e1, Expression e2){
        comparePlans(new Filter(e1, new OneRowRelation()), new Filter(e2, new OneRowRelation()), false);
    }
    protected void comparePlans(
            LogicalPlan plan1,
            LogicalPlan plan2){
        comparePlans(plan1,plan2,true);
    }

    protected void comparePlans(
            LogicalPlan plan1,
            LogicalPlan plan2,
            Boolean checkAnalysis){
        if (checkAnalysis) {
            // Make sure both plan pass checkAnalysis.
            CheckAnalysis.checkAnalysis(plan1);
            CheckAnalysis.checkAnalysis(plan2);
        }


        LogicalPlan p1 = normalizeExprIds(plan1);
        LogicalPlan normalized1 = normalizePlan(normalizeExprIds(plan1));
        LogicalPlan normalized2 = normalizePlan(normalizeExprIds(plan2));
        if (!ParserUtils.equals(normalized1, normalized2)) {
            throw new RuntimeException("== FAIL: Plans do not match ===");
//                    s"""
//                            |== FAIL: Plans do not match ===
//          |${sideBySide(normalized1.treeString, normalized2.treeString).mkString("\n")}
//            """.stripMargin)
        }
    }

    protected LogicalPlan normalizePlan(LogicalPlan plan) {


        return plan.transform(
                new PartialFunction<LogicalPlan, LogicalPlan>(
                        (p) -> {
                            return p instanceof Filter
                                    || p instanceof Sample
                                    || p instanceof Join;
                        },
                        (p) -> {
                            if (p instanceof Filter) {
                                Filter filter = (Filter) p;
                                return new Filter(new And(ParserUtils.sortBy(splitConjunctivePredicates(filter.getCondition()), (q) -> {
                                    return q.hashCode();
                                })), filter.getChild());
                            } else if (p instanceof Sample) {
                                Sample sample = (Sample) p;
                                Sample sample1 = sample.clone();
                                sample1.setSeed(0L);
                                return sample1;
                            } else if (p instanceof Join) {
                                Join join = (Join) p;
                                if (join.getCondition() != null) {
                                    And newCondition = new And(ParserUtils.sortBy(ParserUtils.map(splitConjunctivePredicates(join.getCondition()), (q) -> {
                                        return rewriteEqual(q);
                                    }), (c) -> {
                                        return c.hashCode();
                                    }));
                                    return new Join(join.getLeft(), join.getRight(), join.getJoinType(), newCondition);
                                }
                            }
                            return p;
                        }
                ));
    }

    protected  LogicalPlan normalizeExprIds(LogicalPlan plan) {
        return plan.transformAllExpressions(
                new PartialFunction<>(
                        (q) -> {
                            if (q instanceof ScalarSubquery
                                    || q instanceof Exists
                                    || q instanceof ListQuery
                                    || q instanceof AttributeReference
                                    || q instanceof Alias
                                    || q instanceof AggregateExpression
                                    || q instanceof NamedLambdaVariable)
                                return true;
                            return false;
                        },
                        (q) -> {
                            if (q instanceof ScalarSubquery) {
                                ScalarSubquery s = (ScalarSubquery) q;
                                ScalarSubquery p = s.clone();
                                p.setExprId(new ExprId(0));
                                return p;
                            } else if (q instanceof Exists) {
                                Exists s = (Exists) q;
                                Exists p = s.clone();
                                p.setExprId(new ExprId(0));
                                return p;
                            }else if( q instanceof ListQuery){
                                ListQuery s = (ListQuery)q;
                                ListQuery p = s.clone();
                                p.setExprId(new ExprId(0));
                                return p;
                            }else if( q instanceof AttributeReference){
                                AttributeReference s = (AttributeReference)q;
                                AttributeReference  p = new AttributeReference(s.getName(),s.getDataType(),s.isNullable());
                                p.setExprId(new ExprId(0));
                                return p;
                            }else if( q instanceof Alias){
                                Alias s = (Alias)q;
                                Alias  p = new Alias(s.getChild(),s.getName());
                                p.setExprId(new ExprId(0));
                                return p;
                            }else if( q instanceof AggregateExpression){
                                AggregateExpression s = (AggregateExpression)q;
                                AggregateExpression  p = s.clone();
                                p.setResultId(new ExprId(0));
                                return p;
                            }else if( q instanceof NamedLambdaVariable){
                                NamedLambdaVariable s = (NamedLambdaVariable)q;
                                NamedLambdaVariable  p = s.clone();
                                p.setExprId(new ExprId(0));
                                p.setValue(null);
                                return p;
                            }
                            return q;
                        }
                )

        );
    }


    /**
     * Rewrite [[EqualTo]] and [[EqualNullSafe]] operator to keep order. The following cases will be
     * equivalent:
     * 1. (a = b), (b = a);
     * 2. (a <=> b), (b <=> a).
     */
    private Expression rewriteEqual(Expression condition){
        if(condition instanceof EqualTo){
            EqualTo eq = (EqualTo)condition;
            return new EqualTo(ParserUtils.sortBy(new ArrayList<>(Arrays.asList(eq.getLeft(), eq.getRight())),(q)->{return q.hashCode();}));

        }else if(condition instanceof EqualNullSafe){

            EqualTo eq = (EqualTo)condition;
            return new EqualNullSafe(ParserUtils.sortBy(new ArrayList<>(Arrays.asList(eq.getLeft(), eq.getRight())),(q)->{return q.hashCode();}));
        }else{
            return condition;
        }
    }



}
