package org.apache.spark.sql.catalyst.analysis.unresolved;

import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.logical.LeafNode;

import java.util.List;

/**
 * Created by kenya on 2019/2/21.
 */
public class UnresolvedTableValuedFunction extends LeafNode{
    String functionName;
    List<Expression> functionArgs;
    List<String>outputNames;

    public UnresolvedTableValuedFunction(String functionName,
            List<Expression> functionArgs,
            List<String>outputNames){
        this.functionName = functionName;
        this.functionArgs = functionArgs;
        this.outputNames = outputNames;
    }
}
