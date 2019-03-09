package org.apache.spark.sql.catalyst.expressions;

import lombok.Data;
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult;
import org.apache.spark.sql.catalyst.parser.ParserUtils;
import org.apache.spark.sql.catalyst.trees.TreeNode;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Created by kenya on 2019/1/19.
 */
@Data
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
        for(Expression expression: children()){
            if(!expression.resolved()){
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean equals(Object o){
        return ParserUtils.equals(this,o);
    }


    //TODO:
    //protected abstract ExprCode doGenCode(ctx: CodegenContext, ev: ExprCode);

}
