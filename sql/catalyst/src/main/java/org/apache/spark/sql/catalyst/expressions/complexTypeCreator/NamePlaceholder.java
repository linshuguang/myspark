package org.apache.spark.sql.catalyst.expressions.complexTypeCreator;

import org.apache.spark.sql.catalyst.expressions.LeafExpression;
import org.apache.spark.sql.catalyst.expressions.Unevaluable;

/**
 * Created by kenya on 2019/1/30.
 */
public class NamePlaceholder extends LeafExpression implements Unevaluable {


    @Override
    public boolean equals(Object o){
        return o instanceof NamePlaceholder;
    }
}
