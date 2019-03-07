package org.apache.spark;

import org.apache.spark.scheduler.DAGScheduler;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by kenya on 2019/3/4.
 */
public class SparkContext {
    private static Object SPARK_CONTEXT_CONSTRUCTOR_LOCK = new Object();
    private static AtomicReference<SparkContext>activeContext =
            new AtomicReference<>();

    //private eventProcessLoop = new DAGSchedulerEventProcessLoop(this)

    volatile private DAGScheduler _dagScheduler = null;

    public static SparkContext getActive() {
        synchronized(SPARK_CONTEXT_CONSTRUCTOR_LOCK){
            return activeContext.get();
        }
    }

    public DAGScheduler dagScheduler() {
        return _dagScheduler;
    }
}
