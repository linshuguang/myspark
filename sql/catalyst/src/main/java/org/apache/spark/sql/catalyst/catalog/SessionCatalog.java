package org.apache.spark.sql.catalyst.catalog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry;
import org.apache.spark.sql.catalyst.analysis.TempTableAlreadyExistsException;
import org.apache.spark.sql.catalyst.catalog.functionResources.DummyFunctionResourceLoader;
import org.apache.spark.sql.catalyst.catalog.functionResources.FunctionResourceLoader;
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser;
import org.apache.spark.sql.catalyst.parser.ParserInterface;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.internal.SQLConf;

import javax.annotation.concurrent.GuardedBy;
import java.net.URI;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

/**
 * Created by kenya on 2019/2/26.
 */
public class SessionCatalog {
    Function<Void,ExternalCatalog> externalCatalogBuilder;
    Function<Void,GlobalTempViewManager> globalTempViewManagerBuilder;
    FunctionRegistry functionRegistry;
    SQLConf conf;
    Configuration hadoopConf; //first contact with hadoop common
    ParserInterface parser;
    FunctionResourceLoader functionResourceLoader;

    GlobalTempViewManager globalTempViewManager = null;
    ExternalCatalog externalCatalog;
    String validNameFormat = "([\\w_]+)";

    @GuardedBy("this")
    protected Map<String,LogicalPlan> tempViews = new HashMap<>();

    private GlobalTempViewManager getGlobalTempViewManager(){
        synchronized (this) {
            if (globalTempViewManager == null) {
                globalTempViewManager = globalTempViewManagerBuilder.apply((Void)null);
            }
            return globalTempViewManager;
        }
    }

    private ExternalCatalog getExternalCatalog(){
        synchronized (this) {
            if (externalCatalog == null) {
                externalCatalog = externalCatalogBuilder.apply((Void)null);
            }
            return externalCatalog;
        }
    }

    public SessionCatalog(Function<Void,ExternalCatalog> externalCatalogBuilder,
            Function<Void,GlobalTempViewManager> globalTempViewManagerBuilder,
            FunctionRegistry functionRegistry,
            SQLConf conf,
            Configuration hadoopConf,
            ParserInterface parser,
            FunctionResourceLoader functionResourceLoader){
        this.externalCatalogBuilder = externalCatalogBuilder;
        this.globalTempViewManagerBuilder = globalTempViewManagerBuilder;
        this.functionRegistry = functionRegistry;
        this.conf = conf;
        this.hadoopConf = hadoopConf;
        this.parser = parser;
        this.functionResourceLoader = functionResourceLoader;
    }

    public SessionCatalog(
                ExternalCatalog externalCatalog,
                          FunctionRegistry functionRegistry,
                          SQLConf conf) {
        this(
                (s) ->{ return externalCatalog;},
                (s) -> {return new GlobalTempViewManager("global_temp");},
                functionRegistry,
                conf,
                new Configuration(),
                new CatalystSqlParser(conf),
                new DummyFunctionResourceLoader());

    }

    public void createDatabase(CatalogDatabase dbDefinition, boolean ignoreIfExists){
        String dbName = formatDatabaseName(dbDefinition.name);
        if (dbName == getGlobalTempViewManager().getDatabase()) {
            throw new AnalysisException(
                    "${globalTempViewManager.database} is a system preserved database, " +
                    "you cannot create a database with this name.");
        }
        validateName(dbName);
        try {
            URI qualifiedPath = makeQualifiedPath(dbDefinition.getLocationUri());
            externalCatalog.createDatabase(
                    new CatalogDatabase(dbName, dbDefinition.getDescription(), qualifiedPath, dbDefinition.getProperties()),
                    ignoreIfExists);
        }catch (Exception e){
            //TODO
        }
    }

    protected String formatDatabaseName(String name) {
        if (conf.caseSensitiveAnalysis()){
            return name;
        } else
            return name.toLowerCase(Locale.ROOT);
    }

    private URI makeQualifiedPath(URI path) throws Exception{
        Path hadoopPath = new Path(path);
        FileSystem fs = hadoopPath.getFileSystem(hadoopConf);
        return fs.makeQualified(hadoopPath).toUri();
    }

    private void validateName(String name){
        //TODO
//        if (!validNameFormat.pattern.matcher(name).matches()) {
//            throw new AnalysisException(s"`$name` is not a valid name for tables/databases. " +
//                    "Valid names only contain alphabet characters, numbers and _.")
//        }
    }

    protected String formatTableName(String name){
        if (conf.caseSensitiveAnalysis()) return name; else return name.toLowerCase(Locale.ROOT);
    }

    public void createTempView(
            String name,
            LogicalPlan tableDefinition,
            boolean overrideIfExists){
        synchronized (this) {
            String table = formatTableName(name);
            if (tempViews.containsKey(table) && !overrideIfExists) {
                throw new TempTableAlreadyExistsException(name);
            }
            tempViews.put(table, tableDefinition);
        }
    }

}
