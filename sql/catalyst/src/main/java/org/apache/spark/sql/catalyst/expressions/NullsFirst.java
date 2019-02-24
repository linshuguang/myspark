package org.apache.spark.sql.catalyst.expressions;

/**
 * Created by kenya on 2019/2/21.
 */
public class NullsFirst extends NullOrdering {
    public NullsFirst(){
        super("NULLS FIRST");
    }
}
