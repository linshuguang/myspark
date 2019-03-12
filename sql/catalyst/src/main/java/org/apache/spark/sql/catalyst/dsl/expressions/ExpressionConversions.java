package org.apache.spark.sql.catalyst.dsl.expressions;

import org.apache.spark.lang.Symbol;
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute;
import org.apache.spark.sql.catalyst.expressions.Expression;

/**
 * Created by kenya on 2019/3/10.
 */
public class ExpressionConversions {




    public static UnresolvedAttribute symbolToUnresolvedAttribute(Symbol s){
        return new UnresolvedAttribute(s.name);
    }

    public static Expression convert(Object o){
        if(o instanceof Symbol){
            return symbolToUnresolvedAttribute((Symbol)o);
        }
        return null;
    }





}
