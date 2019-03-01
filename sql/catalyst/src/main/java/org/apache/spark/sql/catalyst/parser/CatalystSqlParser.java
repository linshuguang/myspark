package org.apache.spark.sql.catalyst.parser;

import org.apache.spark.sql.internal.SQLConf;

/**
 * Created by kenya on 2019/2/28.
 */
public class CatalystSqlParser extends AbstractSqlParser {
    SQLConf conf;

    public CatalystSqlParser(SQLConf conf){
        this.conf = conf;
    }
}
