package org.apache.spark.sql.catalyst.expressions;

import org.apache.spark.sql.catalyst.identifiers.FunctionIdentifier;
import org.apache.spark.sql.catalyst.parser.ParserUtils;

import java.util.List;

/**
 * Created by kenya on 2019/2/15.
 */
public class UnresolvedGenerator extends Generator {
    FunctionIdentifier name;
    List<Expression> children;

    public UnresolvedGenerator(FunctionIdentifier name, List<Expression> children){
        this.name = name;
        this.children = children;
    }

    @Override
    protected List<Expression> children(){
        return children;
    }

    @Override
    public boolean equals(Object o){
        if(o instanceof UnresolvedGenerator){
            UnresolvedGenerator u = (UnresolvedGenerator)o;
            return ParserUtils.equals(name,u.name) && ParserUtils.equalList(children,u.children);
        }
        return false;
    }

}
