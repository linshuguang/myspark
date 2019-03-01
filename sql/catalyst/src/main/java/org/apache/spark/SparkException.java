package org.apache.spark;

/**
 * Created by kenya on 2019/3/1.
 */
public class SparkException extends Exception {

    public SparkException(String message, Throwable cause){
        super(message,cause);
    }

    public SparkException(String message){
        super(message,null);
    }


}
