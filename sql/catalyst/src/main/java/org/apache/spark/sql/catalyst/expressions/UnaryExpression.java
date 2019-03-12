package org.apache.spark.sql.catalyst.expressions;

import lombok.Data;
import org.apache.spark.sql.catalyst.parser.ParserUtils;

import java.util.Arrays;
import java.util.List;

/**
 * Created by kenya on 2019/2/14.
 */
@Data
public abstract class UnaryExpression extends Expression {
    Expression child;
    public UnaryExpression(Expression child){
        this.child = child;
    }

    @Override
    protected final List<Expression>children(){
        return Arrays.asList(child);
    }

    @Override
    public boolean equals(Object o){
        if(o instanceof UnaryExpression){
            UnaryExpression u = (UnaryExpression)o;
            return ParserUtils.equals(child,u.child);
        }
        return false;
    }
}
