package org.apache.spark.sql.catalyst.analysis.unresolved;

import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.UnaryExpression;
import org.apache.spark.sql.catalyst.parser.ParserUtils;

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

    @Override
    public boolean equals(Object o){
        if(o instanceof MultiAlias){
            MultiAlias m = (MultiAlias) o;
            return ParserUtils.equals(names,m.names);
        }
        return false;
    }
}
