package org.apache.spark.sql.catalyst.catalog;

import org.apache.spark.sql.catalyst.identifiers.TableIdentifier;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by kenya on 2019/3/1.
 */
public class CatalogTable {
    TableIdentifier identifier;
    CatalogTableType tableType;
    CatalogStorageFormat storage;
    StructType schema;
    String provider;
    List<String> partitionColumnNames;
    BucketSpec bucketSpec;
    String owner;
    long createTime;
    long lastAccessTime;
    String createVersion;
    Map<String,String> properties;
    CatalogStatistics stats;
    String viewText;
    String comment;
    List<String>unsupportedFeatures;
    boolean tracksPartitionsInCatalog;
    boolean schemaPreservesCase;
    Map<String,String>ignoredProperties;
    String viewOriginalText;

    public CatalogTable(TableIdentifier identifier,
            CatalogTableType tableType,
            CatalogStorageFormat storage,
            StructType schema,
            String provider,
            List<String> partitionColumnNames,
            BucketSpec bucketSpec,
            String owner,
            long createTime,
            long lastAccessTime,
            String createVersion,
            Map<String,String> properties,
            CatalogStatistics stats,
            String viewText,
            String comment,
            List<String>unsupportedFeatures,
            boolean tracksPartitionsInCatalog,
            boolean schemaPreservesCase,
            Map<String,String>ignoredProperties,
            String viewOriginalText){
        this.identifier = identifier;
        this.tableType = tableType;
        this.storage = storage;
        this.schema = schema;
        this.provider = provider;
        this.partitionColumnNames = partitionColumnNames;
        this.bucketSpec = bucketSpec;
        this.owner = owner;
        this.createTime = createTime;
        this.lastAccessTime = lastAccessTime;
        this.createVersion = createVersion;
        this.properties = properties;
        this.stats=  stats;
        this.viewText = viewText;
        this.comment = comment;
        this.unsupportedFeatures = unsupportedFeatures;
        this.tracksPartitionsInCatalog = tracksPartitionsInCatalog;
        this.schemaPreservesCase = schemaPreservesCase;
        this.ignoredProperties = ignoredProperties;
        this.viewOriginalText = viewOriginalText;
    }

    public CatalogTable(TableIdentifier identifier,
                        CatalogTableType tableType,
                        CatalogStorageFormat storage,
                        StructType schema){
        this(identifier,tableType,storage,schema,null,new ArrayList<>(),null,"",System.currentTimeMillis(),-1,"",new HashMap<>(),null,null,null,new ArrayList<>(),false,true,new HashMap<>(),null);
    }


}
