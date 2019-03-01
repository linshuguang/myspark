package org.apache.spark.sql.catalyst.catalog;

import lombok.Data;

import java.net.URI;
import java.util.Map;

/**
 * Created by kenya on 2019/2/28.
 */
@Data
public class CatalogDatabase {
    String name;
    String description;
    URI locationUri;
    Map<String,String>properties;

    public CatalogDatabase(String name,
            String description,
            URI locationUri,
            Map<String,String>properties){
        this.name = name;
        this.description = description;
        this.locationUri = locationUri;
        this.properties = properties;
    }
}
