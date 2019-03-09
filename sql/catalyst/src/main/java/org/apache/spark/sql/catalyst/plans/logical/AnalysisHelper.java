package org.apache.spark.sql.catalyst.plans.logical;

import org.apache.spark.sql.catalyst.plans.QueryPlan;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.function.Function;

/**
 * Created by kenya on 2019/2/28.
 */
@Service
public abstract class AnalysisHelper extends QueryPlan<LogicalPlan>{

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

    @Override
    public LogicalPlan transformUp(Function<LogicalPlan, LogicalPlan>rule){
        //assertNotAnalysisRule()
        return super.transformUp(rule);
    }

}
