package org.apache.spark.sql.catalyst.expressions.namedExpressions;


import org.apache.spark.sql.catalyst.expressions.Expression;

/**
 * Created by kenya on 2019/1/30.
 */
public class Alias extends NamedExpression {
    Expression child;
    String name;

    public Alias(Expression child, String name){
        this.child = child;
        this.name = name;
    }

}
