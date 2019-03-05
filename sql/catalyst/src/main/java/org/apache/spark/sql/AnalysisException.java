package org.apache.spark.sql;

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

import java.io.Serializable;

/**
 * Created by kenya on 2019/1/20.
 */
public class AnalysisException extends RuntimeException implements Serializable {
    Integer line;
    Integer startPosition;
    LogicalPlan plan;
    Throwable cause;

    public AnalysisException(String message, Integer line, Integer startPosition, LogicalPlan plan, Throwable cause){
        super(message);
        this.line = line;
        this.startPosition = startPosition;
        this.plan = plan;
        this.cause = cause;
    }

    public AnalysisException(String message){
        this(message, null, null, null, null);
    }

    //public AnalysisException(){}

}
