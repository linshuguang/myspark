package org.apache.spark.scheduler;

import lombok.Data;
import org.apache.spark.MapOutputTrackerMaster;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkEnv;
import org.apache.spark.storage.BlockManagerMaster;
import org.apache.spark.util.EventLoop;
import org.apache.spark.util.clock.Clock;
import org.apache.spark.util.clock.SystemClock;


/**
 * Created by kenya on 2019/3/4.
 */
@Data
public class DAGScheduler {
    SparkContext sc;
    TaskScheduler taskScheduler;
    LiveListenerBus listenerBus;
    MapOutputTrackerMaster mapOutputTracker;
    BlockManagerMaster blockManagerMaster;
    SparkEnv env;
    Clock clock;
    //clock:  = new SystemClock()

    public DAGScheduler(SparkContext sc,
                        TaskScheduler taskScheduler,
                        LiveListenerBus listenerBus,
                        MapOutputTrackerMaster mapOutputTracker,
                        BlockManagerMaster blockManagerMaster,
                        SparkEnv env,
                        Clock clock) {
        this.sc = sc;
        this.taskScheduler = taskScheduler;
        this.listenerBus = listenerBus;
        this.mapOutputTracker = mapOutputTracker;
        this.blockManagerMaster = blockManagerMaster;
        this.env = env;
        this.clock = clock;
    }

    public DAGScheduler(SparkContext sc,
                        TaskScheduler taskScheduler,
                        LiveListenerBus listenerBus,
                        MapOutputTrackerMaster mapOutputTracker,
                        BlockManagerMaster blockManagerMaster,
                        SparkEnv env) {
        this(sc, taskScheduler, listenerBus, mapOutputTracker, blockManagerMaster, env, new SystemClock());
    }

    public static class DAGSchedulerEventProcessLoop extends EventLoop<DAGSchedulerEvent> {
        DAGScheduler dagScheduler;

        public DAGSchedulerEventProcessLoop(DAGScheduler dagScheduler) {
            super("dag-scheduler-event-loop");
            this.dagScheduler = dagScheduler;
        }

        @Override
        protected void onReceive(DAGSchedulerEvent event){

        }

        @Override
        protected void onError(Throwable e){

        }
    }

    private EventLoop eventProcessLoop = new DAGSchedulerEventProcessLoop(this);

}
