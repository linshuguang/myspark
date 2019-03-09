package org.apache.spark.sql.catalyst.expressions;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by kenya on 2019/1/22.
 */
public abstract class LeafExpression extends Expression {
    @Override
    protected final List<Expression> children(){
        return new ArrayList<>();
    }
}
