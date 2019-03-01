package org.apache.spark.sql.catalyst.plans.logical.statistics;

/**
 * Created by kenya on 2019/3/1.
 */
public class Histogram {
    double height;
    HistogramBin[]bins;
    public Histogram(double height,
            HistogramBin[]bins){
        this.height= height;
        this.bins = bins;
    }
}
