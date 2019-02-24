package org.apache.spark.sql.catalyst.analysis.unresolved;

import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.UnaryExpression;

import java.util.List;

/**
 * Created by kenya on 2019/2/21.
 */
public class MultiAlias extends UnaryExpression{
    List<String> names;

    public MultiAlias(Expression child,
            List<String> names){
        super(child);
        this.names = names;
    }
}
