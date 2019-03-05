package org.apache.spark.scheduler;

import org.apache.spark.SparkConf;

/**
 * Created by kenya on 2019/3/4.
 */
public class LiveListenerBus {
    SparkConf conf;
    public LiveListenerBus(SparkConf conf){
        this.conf = conf;
    }
}
