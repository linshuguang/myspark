package org.apache.spark.sql.catalyst.analysis;

import org.apache.spark.sql.catalyst.catalog.SessionCatalog;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.rules.Rule;
import org.apache.spark.sql.catalyst.rules.RuleExecutor;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.util.BeanLoader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by kenya on 2019/3/7.
 */
public class Analyzer extends RuleExecutor<LogicalPlan> {
    SessionCatalog catalog;
    SQLConf conf;
    int maxIterations;


    public Analyzer(SessionCatalog catalog,
            SQLConf conf,
            int maxIterations){
        this.catalog= catalog;
        this.conf = conf;
        this.maxIterations = maxIterations;
    }



    class FixedPoint extends Strategy{
        public FixedPoint(int maxIterations){
            super(maxIterations);
        }
    }

    Strategy fixedPoint = new FixedPoint(maxIterations);





    protected List<Rule<LogicalPlan>>extendedResolutionRules = new ArrayList<>();

    public  Analyzer( SessionCatalog catalog , SQLConf conf){
        this(catalog, conf, conf.optimizerMaxIterations());
    }


    @Override
    public List<Batch> batches(){
        return Arrays.asList(
                new Batch("Cleanup", fixedPoint,new CleanupAliases())
        );
    }

    @Override
    public LogicalPlan execute(LogicalPlan plan){
        AnalysisContext analysisContext = (AnalysisContext)BeanLoader.getBeans(AnalysisContext.class);
        analysisContext.reset();
        try {
            return executeSameContext(plan);
        } finally {
            analysisContext.reset();
        }
    }
    private LogicalPlan executeSameContext(LogicalPlan plan){
        return super.execute(plan);
    }
}
