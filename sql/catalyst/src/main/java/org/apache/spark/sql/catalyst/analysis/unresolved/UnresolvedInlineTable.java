package org.apache.spark.sql.catalyst.analysis.unresolved;

import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.parser.ParserUtils;
import org.apache.spark.sql.catalyst.plans.logical.LeafNode;

import java.util.List;

/**
 * Created by kenya on 2019/2/21.
 */
public class UnresolvedInlineTable extends LeafNode {
    List<String> names;
    List<List<Expression>> rows;

    public UnresolvedInlineTable(List<String> names,
            List<List<Expression>> rows){
        this.names = names;
        this.rows = rows;
    }

    @Override
    public boolean equals(Object o){
        if(o instanceof UnresolvedInlineTable){
            UnresolvedInlineTable  u = (UnresolvedInlineTable)o;
            boolean a = ParserUtils.equalList(names,u.names);
            boolean b = ParserUtils.equalList(rows,u.rows);
            return ParserUtils.equalList(names,u.names) && ParserUtils.equalList(rows,u.rows);
        }
        return false;
    }

}
