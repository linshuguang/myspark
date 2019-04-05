package org.apache.spark.sql.catalyst.expressions;

/**
 * Created by kenya on 2019/2/20.
 */
public class Ascending extends SortDirection {
    public Ascending(){
        super("ASC", new NullsFirst());
    }

    @Override
    public boolean equals(Object o){
        return o instanceof Ascending;
    }
}
