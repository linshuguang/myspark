package org.apache.spark.sql.catalyst.expressions;

import lombok.Data;
import org.apache.spark.sql.catalyst.expressions.predicates.Predicate;
import org.apache.spark.sql.catalyst.parser.ParserUtils;

import java.util.Arrays;
import java.util.List;

/**
 * Created by kenya on 2019/2/21.
 */
@Data
public abstract class BinaryExpression extends Predicate {
    Expression left;
    Expression right;
    public BinaryExpression(Expression left, Expression right){
        this.left = left;
        this.right = right;
    }

    @Override
    protected final List<Expression> children(){
        return Arrays.asList(left, right);
    }


    @Override
    public boolean equals(Object o){
        if(o instanceof BinaryExpression) {
            BinaryExpression b = (BinaryExpression) o;
            return ParserUtils.equals(left,b.left) && ParserUtils.equals(right,b.right);
        }
        return false;
    }

}
