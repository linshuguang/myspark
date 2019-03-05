package org.apache.spark.scheduler;

import org.apache.spark.MapOutputTrackerMaster;
import org.apache.spark.SparkContext;
import org.apache.spark.storage.BlockManagerMaster;

/**
 * Created by kenya on 2019/3/4.
 */
public class DAGScheduler {
    SparkContext sc;
    TaskScheduler taskScheduler;
    LiveListenerBus listenerBus;
    MapOutputTrackerMaster mapOutputTracker;
    BlockManagerMaster blockManagerMaster;
    env: SparkEnv,
    clock: Clock = new SystemClock()
}
