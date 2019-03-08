package org.apache.spark.sql.catalyst.expressions.namedExpressions;


import lombok.Data;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.types.Metadata;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by kenya on 2019/1/30.
 */
@Data
public class Alias extends NamedExpression {
    Expression child;
    String name;
    ExprId exprId;//:  = NamedExpression.newExprId,
    List<String> qualifier;//: Seq[String] = Seq.empty,
    Metadata explicitMetadata;//: Option[Metadata] = None

    public Alias(Expression child, String name){
        this.child = child;
        this.name = name;
        this.exprId = NamedExpression.newExprId();
        this.qualifier = new ArrayList<>();
        this.explicitMetadata = null;
    }

}
