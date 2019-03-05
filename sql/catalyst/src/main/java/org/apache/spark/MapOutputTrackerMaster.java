package org.apache.spark;

import org.apache.spark.broadcast.BroadcastManager;

/**
 * Created by kenya on 2019/3/4.
 */
public class MapOutputTrackerMaster extends MapOutputTracker{
    SparkConf conf;
    BroadcastManager broadcastManager;
    boolean isLocal;
    public MapOutputTrackerMaster(SparkConf conf,
            BroadcastManager broadcastManager,
            boolean isLocal){
        super(conf);
        this.broadcastManager = broadcastManager;
        this.isLocal = isLocal;
    }
}
