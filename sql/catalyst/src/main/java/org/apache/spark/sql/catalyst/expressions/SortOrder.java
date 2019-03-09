package org.apache.spark.sql.catalyst.expressions;

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

}
