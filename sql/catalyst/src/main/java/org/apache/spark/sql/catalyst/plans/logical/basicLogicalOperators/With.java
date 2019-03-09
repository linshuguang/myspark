package org.apache.spark.sql.catalyst.plans.logical.basicLogicalOperators;

import javafx.util.Pair;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias;
import org.apache.spark.sql.catalyst.plans.logical.UnaryNode;
import org.apache.spark.sql.catalyst.util.util;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by kenya on 2019/1/20.
 */
public class With extends UnaryNode {
    List<Pair<String, SubqueryAlias>> cteRelations;

    public With(LogicalPlan child, List<Pair<String, SubqueryAlias>> cteRelations){
        super(child);
        this.cteRelations = cteRelations;
    }

    //@Override
    public String simpleString(){
        List<String> names = new ArrayList<>();
        for(Pair<String, SubqueryAlias> pair: cteRelations){
            names.add(pair.getKey());
        }
        String cteAliases = util.truncatedString(names, "[", ", ", "]");
        return "CTE " + cteAliases;
    }

}
