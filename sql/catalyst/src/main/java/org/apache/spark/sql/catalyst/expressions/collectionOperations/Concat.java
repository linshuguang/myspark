package org.apache.spark.sql.catalyst.expressions.collectionOperations;

import org.apache.spark.sql.catalyst.expressions.ComplexTypeMergingExpression;
import org.apache.spark.sql.catalyst.expressions.Expression;

import java.util.Arrays;
import java.util.List;

/**
 * Created by kenya on 2019/2/22.
 */
public class Concat extends ComplexTypeMergingExpression{
    List<Expression>children;
    public Concat(List<Expression>children){
        this.children = children;
    }

    @Override
    protected List<Expression> children(){
        return children;
    }
}
