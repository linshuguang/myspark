package org.apache.spark.sql.catalyst.expressions;

import lombok.Data;

/**
 * Created by kenya on 2019/2/20.
 */
@Data
public abstract class SortDirection {
    String sql;
    NullOrdering defaultNullOrdering;
    public SortDirection(String sql,NullOrdering defaultNullOrdering){
        this.sql = sql;
        this.defaultNullOrdering = defaultNullOrdering;
    }
}
