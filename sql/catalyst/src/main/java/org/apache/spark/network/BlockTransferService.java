package org.apache.spark.network;

import org.apache.spark.network.shuffle.ShuffleClient;

import java.io.Closeable;

/**
 * Created by kenya on 2019/3/4.
 */
public abstract class BlockTransferService extends ShuffleClient implements Closeable{

}
