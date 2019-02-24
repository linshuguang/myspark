package org.apache.spark.sql.catalyst.expressions.namedExpressions;

import org.apache.spark.sql.catalyst.expressions.subquery.SubqueryExpression;

import java.util.UUID;

/**
 * Created by kenya on 2019/2/22.
 */
public class ExprId {
    long id;
    UUID jvmId;
    public ExprId (long id){
        this(id, NamedExpression.jvmId);
    }
    public ExprId (long id, UUID jvmId){
        this.id = id;
        this.jvmId = jvmId;
    }
}
