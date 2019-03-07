package org.apache.spark;

import org.apache.spark.broadcast.BroadcastManager;
import org.apache.spark.memory.MemoryManager;
import org.apache.spark.metrics.MetricsSystem;
import org.apache.spark.rpc.RpcEnv;
import org.apache.spark.scheduler.OutputCommitCoordinator;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.serializer.SerializerManager;
import org.apache.spark.shuffle.ShuffleManager;
import org.apache.spark.storage.BlockManager;

/**
 * Created by kenya on 2019/3/4.
 */
public class SparkEnv {
    String executorId;
    RpcEnv rpcEnv;
    Serializer serializer;
    Serializer closureSerializer;
    SerializerManager serializerManager;
    MapOutputTracker mapOutputTracker;
    ShuffleManager shuffleManager;
    BroadcastManager broadcastManager;
    BlockManager blockManager;
    SecurityManager securityManager;
    MetricsSystem metricsSystem;
    MemoryManager memoryManager;
    OutputCommitCoordinator outputCommitCoordinator;
    SparkConf conf;
    public SparkEnv(String executorId,
            RpcEnv rpcEnv,
            Serializer serializer,
            Serializer closureSerializer,
            SerializerManager serializerManager,
            MapOutputTracker mapOutputTracker,
            ShuffleManager shuffleManager,
            BroadcastManager broadcastManager,
            BlockManager blockManager,
            SecurityManager securityManager,
            MetricsSystem metricsSystem,
            MemoryManager memoryManager,
            OutputCommitCoordinator outputCommitCoordinator,
            SparkConf conf){
        this.executorId =executorId;
        this.rpcEnv =rpcEnv;
        this.serializer =serializer;
        this.closureSerializer = closureSerializer;
        this.serializerManager = serializerManager;
        this.mapOutputTracker = mapOutputTracker;
        this.shuffleManager = shuffleManager;
        this.broadcastManager = broadcastManager;
        this.blockManager = blockManager;
        this.securityManager= securityManager;
        this.metricsSystem = metricsSystem;
        this.memoryManager = memoryManager;
        this.outputCommitCoordinator = outputCommitCoordinator;
        this.conf = conf;
    }
}
