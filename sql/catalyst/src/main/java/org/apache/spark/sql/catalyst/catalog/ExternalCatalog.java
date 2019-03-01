package org.apache.spark.sql.catalyst.catalog;

/**
 * Created by kenya on 2019/2/26.
 */
public interface ExternalCatalog {
    void createDatabase(CatalogDatabase dbDefinition, boolean ignoreIfExists);
}
