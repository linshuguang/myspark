package org.apache.spark.sql.catalyst.expressions;

/**
 * Created by kenya on 2019/2/21.
 */
public abstract class NullOrdering {
    String sql;
    public NullOrdering(String sql){
        this.sql = sql;
    }
}
