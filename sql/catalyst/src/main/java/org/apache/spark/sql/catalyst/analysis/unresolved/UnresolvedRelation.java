package org.apache.spark.sql.catalyst.analysis.unresolved;

import org.apache.spark.sql.catalyst.identifiers.TableIdentifier;
import org.apache.spark.sql.catalyst.plans.logical.LeafNode;

/**
 * Created by kenya on 2019/2/21.
 */
public class UnresolvedRelation extends LeafNode {
    TableIdentifier tableIdentifier;
    public UnresolvedRelation(TableIdentifier tableIdentifier){
        this.tableIdentifier = tableIdentifier;
    }
}
