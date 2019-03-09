package org.apache.spark.sql.catalyst.plans.logical.basicLogicalOperators;

import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.UnaryNode;

/**
 * Created by kenya on 2019/2/21.
 */
public class InsertIntoDir extends UnaryNode{
    boolean isLocal;
    CatalogStorageFormat storage;
    String provider;
    boolean overwrite;
    public InsertIntoDir(boolean isLocal,
            CatalogStorageFormat storage,
            String provider,
            LogicalPlan child,
            boolean overwrite){
        super(child);
        this.isLocal = isLocal;
        this.storage = storage;
        this.provider = provider;
        this.overwrite = overwrite;
    }
    public InsertIntoDir(boolean isLocal,
                         CatalogStorageFormat storage,
                         String provider,
                         LogicalPlan child){
        this(isLocal, storage, provider, child,true);
    }
}
