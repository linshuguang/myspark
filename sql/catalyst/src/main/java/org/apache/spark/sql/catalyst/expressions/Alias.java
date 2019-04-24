package org.apache.spark.sql.catalyst.expressions;

import org.apache.spark.sql.catalyst.expressions.namedExpressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.namedExpressions.NamedExpression;
import org.apache.spark.sql.types.Metadata;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by kenya on 2019/4/24.
 */
public class Alias extends UnaryExpression {
    String name;

    ExprId exprId = NamedExpression.newExprId();
    List<String> qualifier = new ArrayList<>();
    Metadata explicitMetadata = null;

    public Alias(Expression child, String name){
        super(child);
        this.name = name;
    }


}
