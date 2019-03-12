package org.apache.spark.sql.catalyst.plans;

import org.apache.spark.lang.PartialFunction;
import org.apache.spark.sql.catalyst.expressions.Expression;
import static org.apache.spark.sql.catalyst.parser.ParserUtils.MutableObject;
import org.apache.spark.sql.catalyst.trees.TreeNode;
import org.apache.spark.sql.types.DataType;
import sun.reflect.generics.tree.BaseType;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Created by kenya on 2019/1/18.
 */
public abstract class QueryPlan <PlanType extends QueryPlan<PlanType>> extends TreeNode<PlanType> {

    public PlanType transformAllExpressions(PartialFunction<Expression, Expression> rule){
        return transform(
                new PartialFunction<>(
                        (q)-> {
                            return q instanceof QueryPlan;
                            },
                        (q)->{
                            QueryPlan p = (QueryPlan)q;
                            return (PlanType)p.transformExpressions(rule);
                        }
        ));
    }

    public PlanType transformExpressions(PartialFunction<Expression, Expression> rule){
        return transformExpressionsDown(rule);
    }

    public PlanType transformExpressionsDown(PartialFunction<Expression, Expression>rule){
        return mapExpressions((q)->q.transformDown(rule));
    }


    public PlanType mapExpressions( Function<Expression , Expression> f) {

        MutableObject<Boolean> changed = new MutableObject<>(false);
        Function<Expression,Expression>transformExpression = new Function<Expression, Expression>() {
            @Override
            public Expression apply(Expression e) {
                Expression newE = CurrentOrigin.withOrigin(e.origin(),(q)->{return f.apply(e);}) ;
                if (newE.fastEquals(e)) {
                    return e;
                } else {
                    changed.set(true);
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
                }else{
                    return arg;
                }
            }
        };

        List<Object> newArgs = mapProductIterator(recursiveTransform);

        if (changed.get()) {
            //TODO: pay attention here in cases
            return makeCopy(newArgs);
        } else {
            return (PlanType)this;
        }
    }
}
