package org.apache.spark.sql.catalyst.plans.logical;

import org.apache.spark.sql.catalyst.identifiers.AliasIdentifier;

/**
 * Created by kenya on 2019/1/19.
 */
public class SubqueryAlias extends OrderPreservingUnaryNode{
    AliasIdentifier name;
    LogicalPlan child;

    public SubqueryAlias(String identifier, LogicalPlan child){
        this(new AliasIdentifier(identifier), child);
    }

    public SubqueryAlias(AliasIdentifier name, LogicalPlan child){
        this.name = name;
        this.child = child;
    }

    public String alias(){
        return name.getIdentifier();
    }



}
