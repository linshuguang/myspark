package org.apache.spark.sql.catalyst.expressions.predicates;

import org.apache.spark.sql.catalyst.expressions.Expression;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by kenya on 2019/3/7.
 */
public class PredicateHelper {

    public static List<Expression> splitConjunctivePredicates(Expression condition){
        List<Expression> expressions = new ArrayList<>();
        if(condition instanceof And){
            And and = (And) condition;
            expressions.addAll(splitConjunctivePredicates(and.getLeft()));
            expressions.addAll(splitConjunctivePredicates(and.getRight()));
        }else {
            expressions.add(condition);
        }
        return expressions;
    }
}
