package org.apache.spark;

/**
 * Created by kenya on 2019/3/4.
 */
public abstract class MapOutputTracker {
    SparkConf conf;
    public MapOutputTracker(SparkConf conf){
        this.conf = conf;
    }

}
