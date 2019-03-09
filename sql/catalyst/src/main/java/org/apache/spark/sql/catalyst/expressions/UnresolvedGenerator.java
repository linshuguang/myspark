package org.apache.spark.sql.catalyst.expressions;

import org.apache.spark.sql.catalyst.identifiers.FunctionIdentifier;

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

}
