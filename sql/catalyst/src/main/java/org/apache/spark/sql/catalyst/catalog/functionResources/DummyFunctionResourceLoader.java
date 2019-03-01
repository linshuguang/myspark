package org.apache.spark.sql.catalyst.catalog.functionResources;

/**
 * Created by kenya on 2019/2/28.
 */
public class DummyFunctionResourceLoader implements FunctionResourceLoader {
    @Override
    public void loadResource(FunctionResource resource){
        throw new UnsupportedOperationException();
    }
}
