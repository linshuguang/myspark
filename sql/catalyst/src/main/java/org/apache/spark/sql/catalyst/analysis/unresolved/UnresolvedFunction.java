package org.apache.spark.sql.catalyst.analysis.unresolved;

import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.identifiers.FunctionIdentifier;
import org.apache.spark.sql.catalyst.parser.ParserUtils;

import java.lang.reflect.Parameter;
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

    @Override
    protected List<Expression> children(){
        return children;
    }

    @Override
    public boolean equals(Object o){
        if(o instanceof UnresolvedFunction){
            UnresolvedFunction u = (UnresolvedFunction)o;

            if(!ParserUtils.equals(name,u.name)){
                return false;
            }
            if(!ParserUtils.equalList(children,u.children)){
                return false;
            }
            return isDistinct==u.isDistinct;
        }
        return false;
    }
}
