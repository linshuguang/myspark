package org.apache.spark.sql.catalyst.catalog;

import java.util.List;

/**
 * Created by kenya on 2019/3/1.
 */
public class BucketSpec {
    int numBuckets;
    List<String>bucketColumnNames;
    List<String> sortColumnNames;
    public BucketSpec(int numBuckets,
            List<String>bucketColumnNames,
            List<String> sortColumnNames){
        this.numBuckets = numBuckets;
        this.bucketColumnNames = bucketColumnNames;
        this.sortColumnNames = sortColumnNames;
    }
}
