package org.apache.spark.scheduler;

import org.apache.spark.SparkConf;

/**
 * Created by kenya on 2019/3/5.
 */

public class OutputCommitCoordinator {
    SparkConf conf;
    boolean isDriver;
    public OutputCommitCoordinator(SparkConf conf,
            boolean isDriver){
        this.conf = conf;
        this.isDriver = isDriver;
    }
}
