package org.apache.spark.sql.catalyst.analysis.unresolved;

import org.apache.spark.sql.catalyst.expressions.LeafExpression;

/**
 * Created by kenya on 2019/2/21.
 */
public class Star extends LeafExpression {


    @Override
    public String toString(){
        return "*";
    }
}
