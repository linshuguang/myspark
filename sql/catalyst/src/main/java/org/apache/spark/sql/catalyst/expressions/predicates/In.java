package org.apache.spark.sql.catalyst.expressions.predicates;

import org.apache.spark.sql.catalyst.expressions.Expression;

import java.util.Arrays;
import java.util.List;

/**
 * Created by kenya on 2019/2/22.
 */
public class In extends Predicate {
    Expression value;
    List<Expression> list;
    public In(Expression value,
            List<Expression> list){
        this.value = value;
        this.list = list;
    }

    @Override
    protected List<Expression>children(){
        List<Expression> expressions = Arrays.asList(value);
        expressions.addAll(list);
        return expressions;
    }

}
