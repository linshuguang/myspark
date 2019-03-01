package org.apache.spark.sql.catalyst.catalog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkException;
import org.apache.spark.sql.catalyst.analysis.AlreadyExistException.DatabaseAlreadyExistsException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by kenya on 2019/2/27.
 */
    public class InMemoryCatalog implements ExternalCatalog {
//    conf: SparkConf = new SparkConf,
//    hadoopConfig: Configuration = new Configuration
    SparkConf conf;
    Configuration hadoopConfig;

    private class TableDesc{
        CatalogTable table;

        public TableDesc(CatalogTable table){
            this.table= table;
        }
        Map<Map<String, String>,CatalogTablePartition> partitions = new HashMap<>();
    }

    private class DatabaseDesc{
        CatalogDatabase db;
        public DatabaseDesc(CatalogDatabase db){
            this.db = db;
        }
        Map<String,TableDesc> tables = new HashMap<>();
        Map<String,CatalogFunction> functions = new HashMap<>();
    }

    private Map<String,DatabaseDesc> catalog = new HashMap<>();
    public InMemoryCatalog(SparkConf conf, Configuration hadoopConfig){
        this.conf = conf;
        this.hadoopConfig = hadoopConfig;
    }
    public InMemoryCatalog(){
        this(new SparkConf(), new Configuration());
    }

    @Override
    public void createDatabase(
            CatalogDatabase dbDefinition,
            boolean ignoreIfExists){
        synchronized(this) {
            if (catalog.containsKey(dbDefinition.name)) {
                if (!ignoreIfExists) {
                    throw new DatabaseAlreadyExistsException(dbDefinition.name);
                }
            } else {
                try {
                    Path location = new Path(dbDefinition.locationUri);
                    FileSystem fs = location.getFileSystem(hadoopConfig);
                    fs.mkdirs(location);
                } catch(IOException e) {
                    //TODO
//                        throw new SparkException("Unable to create database ${dbDefinition.name} as failed " +
//                                "to create its directory ${dbDefinition.locationUri}", e);
                }
                catalog.put(dbDefinition.name, new DatabaseDesc(dbDefinition));
            }
        }
    }

}
