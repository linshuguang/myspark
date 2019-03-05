package org.apache.spark.serializer;

import org.apache.spark.SparkConf;

/**
 * Created by kenya on 2019/3/4.
 */
public class SerializerManager {
    Serializer defaultSerializer;
    SparkConf conf;
    Byte[] encryptionKey;

    public SerializerManager(Serializer defaultSerializer,
            SparkConf conf,
            Byte[] encryptionKey){
        this.defaultSerializer = defaultSerializer;
        this.conf = conf;
        this.encryptionKey=encryptionKey;
    }
}
