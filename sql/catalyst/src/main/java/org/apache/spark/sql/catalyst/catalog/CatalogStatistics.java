package org.apache.spark.sql.catalyst.catalog;

import java.math.BigInteger;
import java.util.Map;

/**
 * Created by kenya on 2019/3/1.
 */
public class CatalogStatistics {
    BigInteger sizeInBytes;
    BigInteger rowCount;
    Map<String,CatalogColumnStat> colStats;
    public CatalogStatistics(BigInteger sizeInBytes,
            BigInteger rowCount,
            Map<String,CatalogColumnStat> colStats){
        this.sizeInBytes = sizeInBytes;
        this.rowCount = rowCount;
        this.colStats = colStats;
    }

    public CatalogStatistics(BigInteger sizeInBytes){
        this(sizeInBytes,null, null);
    }
}
