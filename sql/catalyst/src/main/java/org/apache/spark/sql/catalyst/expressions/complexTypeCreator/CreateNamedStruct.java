package org.apache.spark.sql.catalyst.expressions.complexTypeCreator;

import org.apache.spark.sql.catalyst.expressions.Expression;

import java.util.List;

/**
 * Created by kenya on 2019/1/22.
 */
public class CreateNamedStruct extends  CreateNamedStructLike {
    List<Expression>children;

    public CreateNamedStruct(List<Expression>children){
        this.children = children;
    }



}
