package org.apache.spark.sql.catalyst.expressions.namedExpressions;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.LeafExpression;
import org.apache.spark.sql.catalyst.expressions.UnaryExpression;

import javax.naming.Name;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by kenya on 2019/1/30.
 */
@Data
public abstract class NamedExpression extends UnaryExpression {
    String name;
    private static AtomicLong curId = new java.util.concurrent.atomic.AtomicLong();

    public static UUID jvmId = UUID.randomUUID();

    public static ExprId newExprId(){
        return new ExprId(curId.getAndIncrement(), jvmId);
    }

    public NamedExpression(){
        this(null);
    }

    public NamedExpression(Expression child){
        super(child);
    }
    @Override
    public boolean equals(Object o){
        if(o instanceof NamedExpression){
            NamedExpression n = (NamedExpression)o;
            return StringUtils.equals(name,n.name) && jvmId==jvmId;
        }
        return false;
    }
}
