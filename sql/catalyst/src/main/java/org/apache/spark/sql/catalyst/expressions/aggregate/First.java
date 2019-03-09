package org.apache.spark.sql.catalyst.expressions.aggregate;

import org.apache.spark.sql.catalyst.expressions.Expression;

import java.util.Arrays;
import java.util.List;

/**
 * Created by kenya on 2019/2/22.
 */
public class First extends DeclarativeAggregate {

    Expression child;
    Expression ignoreNullsExpr;

    public First(Expression child,
            Expression ignoreNullsExpr){
        this.child = child;
        this.ignoreNullsExpr = ignoreNullsExpr;
    }

    @Override
    protected List<Expression> children(){
        return Arrays.asList(child,ignoreNullsExpr);
    }
}
