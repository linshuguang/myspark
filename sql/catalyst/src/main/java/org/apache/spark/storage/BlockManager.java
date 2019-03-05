package org.apache.spark.storage;

import org.apache.spark.MapOutputTracker;
import org.apache.spark.SecurityManager;
import org.apache.spark.SparkConf;
import org.apache.spark.memory.MemoryManager;
import org.apache.spark.network.BlockDataManager;
import org.apache.spark.network.BlockTransferService;
import org.apache.spark.rpc.RpcEnv;
import org.apache.spark.serializer.SerializerManager;
import org.apache.spark.shuffle.ShuffleManager;

/**
 * Created by kenya on 2019/3/4.
 */

public class BlockManager implements BlockDataManager{
    //TODO: seems important
    String executorId;
    RpcEnv rpcEnv;
    BlockManagerMaster master;
    SerializerManager serializerManager;
    SparkConf conf;
    MemoryManager memoryManager;
    MapOutputTracker mapOutputTracker;
    ShuffleManager shuffleManager;
    BlockTransferService blockTransferService;
    SecurityManager securityManager;
    int numUsableCores;

    public BlockManager(String executorId,
            RpcEnv rpcEnv,
            BlockManagerMaster master,
            SerializerManager serializerManager,
            SparkConf conf,
            MemoryManager memoryManager,
            MapOutputTracker mapOutputTracker,
            ShuffleManager shuffleManager,
            BlockTransferService blockTransferService,
            SecurityManager securityManager,
            int numUsableCores){
        this.executorId = executorId;
        this.rpcEnv = rpcEnv;
        this.master = master;
        this.serializerManager = serializerManager;
        this.conf = conf;
        this.memoryManager = memoryManager;
        this.shuffleManager = shuffleManager;
        this.blockTransferService = blockTransferService;
        this.securityManager = securityManager;
        this.mapOutputTracker = mapOutputTracker;
        this.numUsableCores= numUsableCores;

    }
}
