package org.apache.spark.sql.catalyst.plans;

import org.apache.spark.lang.PartialFunction;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.trees.TreeNode;
import org.apache.spark.sql.types.DataType;
import sun.reflect.generics.tree.BaseType;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Created by kenya on 2019/1/18.
 */
public class QueryPlan <PlanType extends QueryPlan<PlanType>> extends TreeNode<PlanType> {

    public PlanType transformAllExpressions(PartialFunction<Expression, Expression> rule){
        return (PlanType)transform(
                new PartialFunction<>((q)-> {return q instanceof QueryPlan;},(q)->{ QueryPlan p = (QueryPlan)q;return (PlanType)p.transformExpressions(rule);}
        ));
    }

    public PlanType transformExpressions(PartialFunction<Expression, Expression> rule){
        return transformExpressionsDown(rule);
    }

    public PlanType transformExpressionsDown(PartialFunction<Expression, Expression>rule){
        return mapExpressions((q)->q.transformDown(rule));
    }


    public PlanType mapExpressions( Function<Expression , Expression> f) {
        boolean changed = false;

        Function<Expression,Expression>transformExpression = new Function<Expression, Expression>() {
            @Override
            public Expression apply(Expression e) {
                Expression newE = CurrentOrigin.withOrigin(e.origin(),(q)->{return f.apply(e);}) ;
                if (newE.fastEquals(e)) {
                    return e;
                } else {
                    //changed = true;
                    return newE;
                }
            }
        };

        Function<Object,Object> recursiveTransform = new Function<Object, Object>() {
            @Override
            public Object apply(Object arg){
                if(arg instanceof Expression){
                    return transformExpression.apply((Expression)arg);
                }else if(arg instanceof Map || arg instanceof DataType){
                    return arg;
                }
                return null;
            }
        };

        List<Object> newArgs = mapProductIterator(recursiveTransform);

        changed = true;
        if (changed) {
            return makeCopy(newArgs);
        } else {
            return (PlanType)this;
        }
    }
}
