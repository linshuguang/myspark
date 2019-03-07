package org.apache.spark.util.clock;

/**
 * Created by kenya on 2019/3/5.
 */
public interface Clock {
    long getTimeMillis();
    long waitTillTime(long targetTime);
}
