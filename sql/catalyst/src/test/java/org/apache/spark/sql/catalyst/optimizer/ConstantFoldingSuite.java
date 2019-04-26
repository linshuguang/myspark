package org.apache.spark.sql.catalyst.optimizer;

import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases;
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute;
import org.apache.spark.sql.catalyst.optimizer.expressions.*;
import org.apache.spark.sql.catalyst.plans.PlanTest;
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.trees.TreeNode;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import static org.apache.spark.sql.catalyst.dsl.dsl.*;

import java.util.Arrays;
import java.util.List;

/**
 * Created by kenya on 2019/4/7.
 */
@SuppressWarnings("ALL")
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:applicationContext-test.xml"})
public class ConstantFoldingSuite extends PlanTest {


    LocalRelation testRelation = new LocalRelation(applyInt("a"), applyInt("b"),applyInt("c"));

//    class Optimize extends TreeNode<T> {
//        List<Batch> batches = Arrays.asList(
//                new Batch("AnalysisNodes", new Once(),
//                        Arrays.asList(new EliminateSubqueryAliases())),
//                new Batch("ConstantFolding", new Once(),
//                        Arrays.asList(
//                                new OptimizeIn(),
//                                new ConstantFolding(),
//                                new BooleanSimplification()
//                        )
//                ));
//
//
//    }
//
//
//    @Test
//    public void testEliminateSubqueries() {
//        LogicalPlan originalQuery = select(subquery(testRelation,"y"), new UnresolvedAttribute("a"));
//        Optimize optimize = new Optimize();
//        LogicalPlan optimized = optimize.execute(analyze(originalQuery));
//
//        LogicalPlan correctAnswer = analyze(select(testRelation, attr("a")));
//        comparePlans(optimized, correctAnswer);
//    }



}
