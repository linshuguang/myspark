package org.apache.spark.sql.catalyst.expressions.conditionalExpressions;

import javafx.util.Pair;
import org.apache.spark.sql.catalyst.expressions.ComplexTypeMergingExpression;
import org.apache.spark.sql.catalyst.expressions.Expression;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by kenya on 2019/2/22.
 */
public class CaseWhen extends ComplexTypeMergingExpression {
    List<Pair<Expression, Expression>> branches;
    Expression elseValue;

    public CaseWhen(List<Pair<Expression, Expression>> branches,
            Expression elseValue){
        this.branches = branches;
        this.elseValue = elseValue;
    }

    @Override
    protected List<Expression> children(){
        //branches.flatMap(b => b._1 :: b}._2 :: Nil) ++ elseValue
        List<Expression> list = new ArrayList<>();
        for(Pair<Expression,Expression>pair:branches){
            list.add(pair.getKey());
            list.add(pair.getValue());
        }
        list.add(elseValue);
        return list;
    }
}
