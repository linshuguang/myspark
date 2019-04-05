package org.apache.spark.sql.catalyst.expressions.predicates;

import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.subquery.ListQuery;
import org.apache.spark.sql.catalyst.parser.ParserUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by kenya on 2019/2/22.
 */
public class InSubquery extends Predicate {
    List<Expression> values;
    ListQuery query;

    public InSubquery(List<Expression> values,
            ListQuery query){
        this.values = values;
        this.query = query;
    }

    @Override
    protected List<Expression> children(){
        List<Expression> list = new ArrayList<>();
        list.addAll(values);
        list.add(query);
        return list;
    }

    @Override
    public boolean equals(Object o){
        if(o instanceof In){
            InSubquery in = (InSubquery)o;
            return ParserUtils.equals(query,in.query) && ParserUtils.equalList(values, in.values);
        }
        return false;
    }
}
