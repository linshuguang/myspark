package org.apache.spark.sql.catalyst.catalog.functionResources;

/**
 * Created by kenya on 2019/2/26.
 */
public interface FunctionResourceLoader {
    void loadResource(FunctionResource resource);
}
