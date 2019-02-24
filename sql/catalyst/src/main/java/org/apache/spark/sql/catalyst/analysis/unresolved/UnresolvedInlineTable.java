package org.apache.spark.sql.catalyst.analysis.unresolved;

import org.apache.spark.sql.catalyst.expressions.Expression;
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

}
