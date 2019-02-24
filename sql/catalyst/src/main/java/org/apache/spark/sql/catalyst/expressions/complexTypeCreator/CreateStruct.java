package org.apache.spark.sql.catalyst.expressions.complexTypeCreator;

import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.namedExpressions.NamedExpression;
import org.apache.spark.sql.catalyst.expressions.literals.Literal;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by kenya on 2019/1/22.
 */
public class CreateStruct implements FunctionBuilder {



    @Override
    public CreateNamedStruct apply(List<Expression> children){
        return build(children);
    }

    public static CreateNamedStruct build(List<Expression> children){
        int index = 0;
        List<Expression> expr = new ArrayList<>();
        for(Expression e: children){
            if(e instanceof NamedExpression){
                NamedExpression ne = (NamedExpression)e;
                if(ne.resolved()){
                    expr.add(Literal.build(ne.getName()));
                }else{
                    expr.add(new NamePlaceholder());
                }
            }else{
                expr.add(Literal.build("col"+(index+1)));
            }
            expr.add(e);
            index++;
        }
        return new CreateNamedStruct(expr);
    }

}
