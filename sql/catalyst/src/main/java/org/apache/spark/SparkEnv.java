package org.apache.spark;

import org.apache.spark.broadcast.BroadcastManager;
import org.apache.spark.rpc.RpcEnv;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.serializer.SerializerManager;
import org.apache.spark.shuffle.ShuffleManager;

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
    val blockManager: BlockManager,
    val securityManager: SecurityManager,
    val metricsSystem: MetricsSystem,
    val memoryManager: MemoryManager,
    val outputCommitCoordinator: OutputCommitCoordinator,
    val conf: SparkConf
}
