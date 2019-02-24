package org.apache.spark.sql.catalyst.analysis.unresolved;

import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.identifiers.FunctionIdentifier;

import java.util.List;

/**
 * Created by kenya on 2019/2/22.
 */
public class UnresolvedFunction extends Expression {
    FunctionIdentifier name;
    List<Expression> children;
    boolean isDistinct;
    public UnresolvedFunction(FunctionIdentifier name,
            List<Expression> children,
            boolean isDistinct){
        this.name = name;
        this.children = children;
        this.isDistinct = isDistinct;
    }

    public UnresolvedFunction(String name,
                              List<Expression> children,
                              boolean isDistinct){
        this(new FunctionIdentifier(name, null),children, isDistinct);
    }
}
