package org.apache.spark.sql.catalyst.trees;

import org.apache.spark.SparkFunSuite;
import org.apache.spark.lang.PartialFunction;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.literals.Literal;
import org.apache.spark.sql.catalyst.rules.Rule;
import org.apache.spark.sql.catalyst.rules.RuleExecutor;
import org.apache.spark.sql.types.IntegerType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Arrays;
import java.util.List;

/**
 * Created by kenya on 2019/4/25.
 */
@SuppressWarnings("ALL")
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:applicationContext-test.xml"})
public class RuleExecutorSuite  extends SparkFunSuite {

    class DecrementLiterals extends Rule<Expression> {
        @Override
        public Expression apply(Expression e) {
            return e.transform(new PartialFunction<>(
                    (p) -> {
                        if (p instanceof Literal) {
                            Literal literal = (Literal) p;
                            if (literal.getDataType() instanceof IntegerType) {
                                return true;
                            }
                            return false;
                        } else {
                            return false;
                        }
                    },
                    (p) -> {
                        Literal literal = (Literal) p;
                        Integer i = (Integer) literal.getValue();
                        return Literal.build(i-1);
                    }
            ));

        }
    }


    @Test
    public void testOnlyOnce() {

        class ApplyOnce extends RuleExecutor<Expression> {
            public List<Batch> batches(){
                return Arrays.asList(
                        new Batch("once", new Once(),new DecrementLiterals())
                );
            }
        }

        ApplyOnce once = new ApplyOnce();
        int i = 10;
        Literal l1 = Literal.build(i);
        Literal l2 = Literal.build(i-1);
        assert once.execute(l1) == l2;
    }

//    test("to fixed point") {
//        object ToFixedPoint extends RuleExecutor[Expression] {
//            val batches = Batch("fixedPoint", FixedPoint(100), DecrementLiterals) :: Nil
//        }
//
//        assert(ToFixedPoint.execute(Literal(10)) === Literal(0))
//    }
//
//    test("to maxIterations") {
//        object ToFixedPoint extends RuleExecutor[Expression] {
//            val batches = Batch("fixedPoint", FixedPoint(10), DecrementLiterals) :: Nil
//        }
//
//        val message = intercept[TreeNodeException[LogicalPlan]] {
//            ToFixedPoint.execute(Literal(100))
//        }.getMessage
//        assert(message.contains("Max iterations (10) reached for batch fixedPoint"))
//    }
//
//    test("structural integrity checker") {
//        object WithSIChecker extends RuleExecutor[Expression] {
//            override protected def isPlanIntegral(expr: Expression): Boolean = expr match {
//                case IntegerLiteral(_) => true
//                case _ => false
//            }
//            val batches = Batch("once", Once, DecrementLiterals) :: Nil
//        }
//
//        assert(WithSIChecker.execute(Literal(10)) === Literal(9))
//
//        val message = intercept[TreeNodeException[LogicalPlan]] {
//            WithSIChecker.execute(Literal(10.1))
//        }.getMessage
//        assert(message.contains("the structural integrity of the plan is broken"))
//    }
}


