package org.apache.spark.sql.catalyst.expressions.stringExpressions;

import jdk.nashorn.internal.ir.TernaryNode;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.TernaryExpression;
import org.apache.spark.sql.catalyst.expressions.literals.Literal;

import java.util.Arrays;
import java.util.List;

/**
 * Created by kenya on 2019/2/22.
 */
public class StringLocate extends TernaryExpression{
    Expression substr;
    Expression str;
    Expression start;
    public StringLocate(Expression substr,
            Expression str,
            Expression start){
        this.substr = substr;
        this.str = str;
        this.start = start;
    }

    @Override
    protected List<Expression> children(){
        return Arrays.asList(substr,str,start);
    }

    public StringLocate(Expression substr, Expression str){
        this(substr, str, Literal.build(new Integer(1)));
    }


}
