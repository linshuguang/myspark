package org.apache.spark.sql.catalyst.analysis;

import org.apache.spark.sql.catalyst.catalog.SessionCatalog;
import org.apache.spark.sql.catalyst.rules.RuleExecutor;
import org.apache.spark.sql.internal.SQLConf;

/**
 * Created by kenya on 2019/3/7.
 */
public class Analyzer extends RuleExecutor {
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

    public  Analyzer( SessionCatalog catalog , SQLConf conf){
        this(catalog, conf, conf.optimizerMaxIterations());
    }
}
