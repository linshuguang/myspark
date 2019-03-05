package org.apache.spark;

import java.io.Serializable;

/**
 * Created by kenya on 2019/3/4.
 */
public abstract class TaskContext implements Serializable{

    private static ThreadLocal<TaskContext> taskContext  = new ThreadLocal<>();

    public static TaskContext get(){
        return taskContext.get();
    }

}
