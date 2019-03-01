package org.apache.spark.sql.catalyst.expressions;

/**
 * Created by kenya on 2019/2/27.
 */
public abstract class RuntimeReplaceable extends UnaryExpression {
    public RuntimeReplaceable(){
        super(null);
    }
}
