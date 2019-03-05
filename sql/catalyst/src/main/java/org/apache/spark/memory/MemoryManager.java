package org.apache.spark.memory;

import org.apache.spark.SparkConf;

/**
 * Created by kenya on 2019/3/4.
 */
public abstract class MemoryManager {
    SparkConf conf;
    int numCores;
    long onHeapStorageMemory;
    long onHeapExecutionMemory;

    public MemoryManager(SparkConf conf,
            int numCores,
            long onHeapStorageMemory,
            long onHeapExecutionMemory){
        this.conf = conf;
        this.numCores = numCores;
                this.onHeapStorageMemory = onHeapStorageMemory;
                this.onHeapExecutionMemory = onHeapExecutionMemory;
    }

}
