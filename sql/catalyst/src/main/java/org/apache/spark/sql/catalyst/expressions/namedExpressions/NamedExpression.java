package org.apache.spark.sql.catalyst.expressions.namedExpressions;

import lombok.Data;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.LeafExpression;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by kenya on 2019/1/30.
 */
@Data
public abstract class NamedExpression extends LeafExpression{
    String name;
    private static AtomicLong curId = new java.util.concurrent.atomic.AtomicLong();

    public static UUID jvmId = UUID.randomUUID();

    public static ExprId newExprId(){
        return new ExprId(curId.getAndIncrement(), jvmId);
    }
}
