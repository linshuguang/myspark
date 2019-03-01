package org.apache.spark.sql.catalyst.analysis;

import lombok.Data;
import org.apache.spark.sql.catalyst.catalog.SessionCatalog;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.rules.RuleExecutor;
import org.apache.spark.sql.catalyst.trees.Rule;
import org.apache.spark.sql.internal.SQLConf;

import java.util.List;

/**
 * Created by kenya on 2019/2/26.
 */
@Data
public class Analyzer extends RuleExecutor<LogicalPlan> {
    SessionCatalog catalog;
    SQLConf conf;
    int maxIterations;

    public Analyzer(SessionCatalog catalog,
            SQLConf conf,
            int maxIterations){
        this.catalog = catalog;
        this.conf = conf;
        this.maxIterations = maxIterations;
    }

    public Analyzer(SessionCatalog catalog, SQLConf conf){
        this(catalog, conf, conf.optimizerMaxIterations());
    }

    List<Rule<LogicalPlan>> extendedResolutionRules=null;



}
