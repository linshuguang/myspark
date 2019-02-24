package org.apache.spark.sql.catalyst.expressions;

/**
 * Created by kenya on 2019/2/21.
 */
public class NullsLast extends NullOrdering {
    public NullsLast(){
        super("NULLS LAST");
    }
}
