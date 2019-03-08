package org.apache.spark.sql.catalyst.analysis;

import javafx.util.Pair;
import org.apache.spark.sql.catalyst.catalog.InMemoryCatalog;
import org.apache.spark.sql.catalyst.catalog.SessionCatalog;
import org.apache.spark.sql.internal.SQLConf;

/**
 * Created by kenya on 2019/3/7.
 */
public class SimpleAnalyzer extends Analyzer {
    public SimpleAnalyzer(){
        super(new SessionCatalog(
                        new InMemoryCatalog(),
                        new EmptyFunctionRegistry(),
                new SQLConf().copy(new Pair<>(SQLConf.CASE_SENSITIVE , true))),
                new SQLConf().copy(new Pair<>(SQLConf.CASE_SENSITIVE , true))
                );
    }


}
