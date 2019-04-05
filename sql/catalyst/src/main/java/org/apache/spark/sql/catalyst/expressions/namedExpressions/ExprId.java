package org.apache.spark.sql.catalyst.expressions.namedExpressions;

import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.subquery.SubqueryExpression;

import java.io.Serializable;
import java.util.UUID;

/**
 * Created by kenya on 2019/2/22.
 */
public class ExprId implements Serializable{
    long id;
    UUID jvmId;
    public ExprId (long id){
        this(id, NamedExpression.jvmId);
    }
    public ExprId (long id, UUID jvmId){
        this.id = id;
        this.jvmId = jvmId;
    }

    @Override
    public boolean equals(Object o){
        if(o instanceof ExprId){
            ExprId e = (ExprId)o;
            return this.jvmId==e.jvmId;
        }
        return false;
    }
}
