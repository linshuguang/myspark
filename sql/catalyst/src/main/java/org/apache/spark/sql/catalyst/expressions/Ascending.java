package org.apache.spark.sql.catalyst.expressions;

/**
 * Created by kenya on 2019/2/20.
 */
public class Ascending extends SortDirection {
    public Ascending(){
        super("ASC", new NullsFirst());
    }
}
