package org.apache.spark.sql.catalyst.expressions;

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult;
import org.apache.spark.sql.catalyst.trees.TreeNode;

/**
 * Created by kenya on 2019/1/19.
 */
public abstract class Expression extends TreeNode<Expression> {

    boolean foldable = false;

    boolean nullable;

    public boolean resolved(){
        return childrenResolved() && checkInputDataTypes().isSuccess();
    }

    public TypeCheckResult checkInputDataTypes(){
        return new TypeCheckResult.TypeCheckSuccess();
    }

    public boolean childrenResolved(){
        for(Expression expression: this.children){
            if(!expression.resolved()){
                return false;
            }
        }
        return true;
    }


}
