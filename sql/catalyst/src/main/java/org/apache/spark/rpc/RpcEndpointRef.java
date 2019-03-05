package org.apache.spark.rpc;

import org.apache.spark.SparkConf;

/**
 * Created by kenya on 2019/3/4.
 */
public abstract class RpcEndpointRef {
    SparkConf conf;
    public RpcEndpointRef(SparkConf conf){
        this.conf = conf;
    }
}
