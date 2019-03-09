package org.apache.spark.sql.catalyst.expressions.windowExpressions;

import org.apache.spark.sql.catalyst.expressions.Expression;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by kenya on 2019/2/22.
 */
public abstract class WindowFrame extends Expression {

    @Override
    protected List<Expression>children(){
        return new ArrayList<>();
    }
}
