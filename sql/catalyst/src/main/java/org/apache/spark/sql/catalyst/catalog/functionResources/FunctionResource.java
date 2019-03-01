package org.apache.spark.sql.catalyst.catalog.functionResources;

/**
 * Created by kenya on 2019/2/26.
 */
public class FunctionResource {
    FunctionResourceType resourceType;
    String uri;

    public FunctionResource( FunctionResourceType resourceType,
            String uri){
        this.resourceType = resourceType;
        this.uri = uri;
    }
}
