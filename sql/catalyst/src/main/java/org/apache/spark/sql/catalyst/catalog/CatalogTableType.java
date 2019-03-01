package org.apache.spark.sql.catalyst.catalog;

/**
 * Created by kenya on 2019/3/1.
 */
public class CatalogTableType {
    String name;

    public static CatalogTableType EXTERNAL = new CatalogTableType("EXTERNAL");
    public static CatalogTableType MANAGED = new CatalogTableType("MANAGED");
    public static CatalogTableType VIEW = new CatalogTableType("VIEW");

    public CatalogTableType(String name){
        this.name = name;
    }
}
