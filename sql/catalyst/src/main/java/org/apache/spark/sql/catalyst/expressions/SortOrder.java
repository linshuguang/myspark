package org.apache.spark.sql.catalyst.expressions;

import org.apache.spark.sql.catalyst.parser.ParserUtils;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by kenya on 2019/2/18.
 */
public class SortOrder extends UnaryExpression{

    SortDirection direction;
    NullOrdering nullOrdering;
    Set<Expression> sameOrderExpressions;

    public SortOrder(Expression child,
            SortDirection direction,
            NullOrdering nullOrdering,
            Set<Expression> sameOrderExpressions){
        super(child);
        this.direction = direction;
        this.nullOrdering = nullOrdering;
        this.sameOrderExpressions = sameOrderExpressions;
    }

    public SortOrder(Expression child,
                     SortDirection direction,
                     NullOrdering nullOrdering){
        this(child, direction, nullOrdering, new HashSet<>());
    }

    public SortOrder(Expression child,
                     SortDirection direction){
        this(child, direction, direction.getDefaultNullOrdering());
    }

    @Override
    public boolean equals(Object o){
        if(o instanceof SortOrder){
            SortOrder s = (SortOrder)o;
            if(!ParserUtils.equals(direction,s.direction)){
                return false;
            }
            if(!ParserUtils.equals(nullOrdering,s.nullOrdering)){
                return false;
            }

            if(sameOrderExpressions!=null && s.sameOrderExpressions==null ) {
              return false;
            }
            if(sameOrderExpressions==null && s.sameOrderExpressions!=null ) {
                return false;
            }
            if(sameOrderExpressions!=null && s.sameOrderExpressions!=null ) {
                if(sameOrderExpressions.size()!=s.sameOrderExpressions.size()){
                    return false;
                }
                Set<Expression> se = new HashSet<>();

                for (Expression e : sameOrderExpressions) {
                    for (Expression e1 : s.sameOrderExpressions) {
                        if (ParserUtils.equals(e, e1)) {
                            se.add(e);
                        }
                    }
                }
                if(se.size()!=sameOrderExpressions.size()){
                    return false;
                }
            }
            return super.equals(o);
        }
        return false;
    }

}
