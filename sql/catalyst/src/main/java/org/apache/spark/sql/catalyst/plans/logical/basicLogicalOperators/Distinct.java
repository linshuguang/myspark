package org.apache.spark.sql.catalyst.plans.logical.basicLogicalOperators;

import org.apache.spark.sql.catalyst.parser.ParserUtils;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.UnaryNode;

/**
 * Created by kenya on 2019/2/18.
 */
public class Distinct extends UnaryNode {

    public Distinct(LogicalPlan child){
        super(child);
    }

    @Override
    public boolean equals(Object o){
        if(o instanceof Distinct){
            Distinct d = (Distinct)o;
            return ParserUtils.equals(getChild(),d.getChild());
        }
        return false;
    }
}
