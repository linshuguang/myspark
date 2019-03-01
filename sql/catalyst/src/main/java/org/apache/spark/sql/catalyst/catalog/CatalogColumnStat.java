package org.apache.spark.sql.catalyst.catalog;

import org.apache.spark.sql.catalyst.plans.logical.statistics.Histogram;

import java.math.BigInteger;

/**
 * Created by kenya on 2019/3/1.
 */
public class CatalogColumnStat {
    BigInteger distinctCount;
    String min;
    String max;
    BigInteger nullCount;
    Long avgLen;
    Long maxLen;
    Histogram histogram;
    public CatalogColumnStat(BigInteger distinctCount,
            String min,
            String max,
            BigInteger nullCount,
            Long avgLen,
                             Long maxLen,
            Histogram histogram){
        this.distinctCount = distinctCount;
        this.min = min;
        this.max = max;
        this.nullCount = nullCount;
        this.avgLen = avgLen;
        this.maxLen = maxLen;
        this.histogram = histogram;
    }
    public CatalogColumnStat(){
        this(null,null,null,null,null,null,null);
    }
}
