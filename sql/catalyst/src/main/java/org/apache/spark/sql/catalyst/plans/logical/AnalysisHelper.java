package org.apache.spark.sql.catalyst.plans.logical;

import org.apache.spark.sql.catalyst.plans.QueryPlan;

import java.util.function.Function;

/**
 * Created by kenya on 2019/2/28.
 */
public class AnalysisHelper extends QueryPlan<LogicalPlan>{

    private static ThreadLocal<Integer> resolveOperatorDepth = new ThreadLocal<Integer>(){
        @Override
        public Integer initialValue(){
            return 0;
        }
    };

    public static <T> T allowInvokingTransformsInAnalyzer(Function<Void,T> f){
        resolveOperatorDepth.set(resolveOperatorDepth.get() + 1);
        try {
            return f.apply((Void)null);
        } finally {
            resolveOperatorDepth.set(resolveOperatorDepth.get() - 1);
        }
    }

//    @Override
//    public LogicalPlan transformUp(rule: PartialFunction[LogicalPlan, LogicalPlan]): LogicalPlan = {
//        assertNotAnalysisRule()
//        super.transformUp(rule)
//    }
}
