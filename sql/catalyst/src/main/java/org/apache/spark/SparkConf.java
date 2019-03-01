package org.apache.spark;

import java.io.Serializable;

/**
 * Created by kenya on 2019/2/27.
 */
public class SparkConf implements Serializable,Cloneable {
    boolean loadDefaults;

    public SparkConf(boolean loadDefaults){
        this.loadDefaults = loadDefaults;
    }

    public SparkConf(){
        this(true);
    }
}
