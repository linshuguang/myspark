package org.apache.spark.sql.catalyst.expressions;

import lombok.Data;

/**
 * Created by kenya on 2019/2/21.
 */
@Data
public class BinaryExpression extends Expression  {
    Expression left;
    Expression right;
    public BinaryExpression(Expression left, Expression right){
        this.left = left;
        this.right = right;
    }

}
