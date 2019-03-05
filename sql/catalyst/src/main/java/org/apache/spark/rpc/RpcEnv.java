package org.apache.spark.rpc;

import org.apache.spark.SparkConf;

/**
 * Created by kenya on 2019/3/4.
 */
public abstract class RpcEnv {
    SparkConf conf;
    public RpcEnv(SparkConf conf){
        this.conf = conf;
    }
}
