package org.apache.spark.sql.catalyst.plans.logical;

import org.apache.spark.sql.catalyst.identifiers.AliasIdentifier;
import org.apache.spark.sql.catalyst.parser.ParserUtils;

/**
 * Created by kenya on 2019/1/19.
 */
public class SubqueryAlias extends OrderPreservingUnaryNode{
    AliasIdentifier name;

    public SubqueryAlias(String identifier, LogicalPlan child){
        this(new AliasIdentifier(identifier), child);
    }

    public SubqueryAlias(AliasIdentifier name, LogicalPlan child){
        super(child);
        this.name = name;
    }

    public String alias(){
        return name.getIdentifier();
    }


    @Override
    public boolean equals(Object o){
        if(o instanceof SubqueryAlias){
            SubqueryAlias s = (SubqueryAlias)o;
            return ParserUtils.equals(name,s.name);
        }
        return false;
    }


}
