package org.apache.spark.sql.catalyst.expressions;

/**
 * Created by kenya on 2019/2/20.
 */
public class Descending extends SortDirection{
    public Descending(){
        super("DESC", new NullsLast());
    }

    @Override
    public boolean equals(Object o){
        return o instanceof Descending;
    }
}
