package org.apache.spark.sql.catalyst.analysis.unresolved;

import org.apache.spark.sql.catalyst.identifiers.TableIdentifier;
import org.apache.spark.sql.catalyst.parser.ParserUtils;
import org.apache.spark.sql.catalyst.plans.logical.LeafNode;

/**
 * Created by kenya on 2019/2/21.
 */
public class UnresolvedRelation extends LeafNode {
    TableIdentifier tableIdentifier;
    public UnresolvedRelation(TableIdentifier tableIdentifier){
        this.tableIdentifier = tableIdentifier;
    }

    @Override
    public boolean equals(Object o){
        if(o instanceof UnresolvedRelation){
            UnresolvedRelation u = (UnresolvedRelation)o;
            return ParserUtils.equals(tableIdentifier,u.tableIdentifier);
        }
        return false;
    }
}
