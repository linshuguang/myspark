package org.apache.spark.sql.catalyst.expressions;

/**
 * Created by kenya on 2019/2/21.
 */
public class Or extends BinaryOperator {

    public Or(Expression left, Expression right){
        super(left, right);
    }

}

