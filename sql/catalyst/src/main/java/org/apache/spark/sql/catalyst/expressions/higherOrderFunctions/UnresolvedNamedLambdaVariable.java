package org.apache.spark.sql.catalyst.expressions.higherOrderFunctions;

import org.apache.spark.sql.catalyst.expressions.LeafExpression;
import org.apache.spark.sql.catalyst.expressions.namedExpressions.NamedExpression;

import java.util.List;

/**
 * Created by kenya on 2019/2/22.
 */
public class UnresolvedNamedLambdaVariable extends NamedExpression {
    List<String> nameParts;
    public UnresolvedNamedLambdaVariable(List<String> nameParts){
        this.nameParts = nameParts;
    }
}
