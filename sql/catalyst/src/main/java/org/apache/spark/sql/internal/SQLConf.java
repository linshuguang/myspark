package org.apache.spark.sql.internal;

import javafx.util.Pair;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.catalyst.parser.ParserInterface;
import org.apache.spark.sql.catalyst.parser.ParserUtils;
import org.apache.spark.sql.internal.config.ConfigBuilder;
import org.apache.spark.sql.internal.config.ConfigEntry;
import org.apache.spark.sql.internal.config.ConfigReader;
import org.apache.spark.util.Utils;
import org.springframework.stereotype.Service;

import java.beans.Transient;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Created by kenya on 2019/1/18.
 */

public class SQLConf implements Serializable {

    public static Map<String,Object> sqlConfEntries  = Collections.synchronizedMap(new HashMap<String,Object>(1));



    transient static protected Map<String,String> settings = java.util.Collections.synchronizedMap(
            new HashMap<>());

    transient static protected ConfigReader reader = new ConfigReader(settings);

    private static ThreadLocal<SQLConf> fallbackConf = new ThreadLocal<SQLConf>() {
        @Override
        public SQLConf initialValue(){
            return new SQLConf();
        }
    };


    public static ConfigEntry<Boolean> LEGACY_HAVING_WITHOUT_GROUP_BY_AS_WHERE = buildConf("spark.sql.legacy.parser.havingWithoutGroupByAsWhere")
        .internal()
        .doc("If it is set to true, the parser will treat HAVING without GROUP BY as a normal WHERE, which does not follow SQL standard.")
        .booleanConf().createWithDefault(false);

    public static ConfigEntry<Integer> OPTIMIZER_MAX_ITERATIONS = buildConf("spark.sql.optimizer.maxIterations")
        .internal()
        .doc("The max number of iterations the optimizer and analyzer runs.")
        .intConf().createWithDefault(100);

    public static ConfigEntry<Boolean> LEGACY_SETOPS_PRECEDENCE_ENABLED = buildConf("spark.sql.legacy.setopsPrecedence.enabled")
        .internal()
        .doc("When set to true and the order of evaluation is not specified by parentheses, the " +
                   "set operations are performed from left to right as they appear in the query. When set " +
                   "to false and order of evaluation is not specified by parentheses, INTERSECT operations " +
                   "are performed before any UNION, EXCEPT and MINUS operations.")
        .booleanConf()
        .createWithDefault(false);

    public static ConfigEntry<Boolean> CASE_SENSITIVE = buildConf("spark.sql.caseSensitive")
        .internal()
        .doc("Whether the query analyzer should be case sensitive or not. " +
                 "Default to case insensitive. It is highly discouraged to turn on case sensitive mode.")
        .booleanConf()
        .createWithDefault(false);

    public static  ConfigEntry<Boolean> SUPPORT_QUOTED_REGEX_COLUMN_NAME = buildConf("spark.sql.parser.quotedRegexColumnNames")
            .doc("When true, quoted Identifiers (using backticks) in SELECT statement are interpreted" +
                    " as regular expressions.")
            .booleanConf()
            .createWithDefault(false);

    public static  ConfigEntry<Boolean> ESCAPED_STRING_LITERALS = buildConf("spark.sql.parser.escapedStringLiterals")
            .internal()
            .doc("When true, string literals (including regex patterns) remain escaped in our SQL " +
                    "parser. The default is false since Spark 2.0. Setting it to true can restore the behavior " +
                    "prior to Spark 2.0.")
            .booleanConf()
            .createWithDefault(false);

    public static  ConfigEntry<String> OPTIMIZER_PLAN_CHANGE_LOG_LEVEL = buildConf("spark.sql.optimizer.planChangeLog.level")
            .internal()
            .doc("Configures the log level for logging the change from the original plan to the new " +
                    "plan after a rule is applied. The value can be 'trace', 'debug', 'info', 'warn', or " +
                    "'error'. The default log level is 'trace'.")
            .stringConf()
            .transform((s)->{return s.toUpperCase(Locale.ROOT);})
            .checkValue((logLevel)->{
                    String level = logLevel.toString();
                        return   StringUtils.equals(level,"TRACE") ||
                        StringUtils.equals(level,"DEBUG") ||
                        StringUtils.equals(level,"INFO") ||
                        StringUtils.equals(level,"WARN") ||
                        StringUtils.equals(level,"ERROR");},
      "Invalid value for 'spark.sql.optimizer.planChangeLog.level'. Valid values are " +
              "'trace', 'debug', 'info', 'warn' and 'error'.")
              .createWithDefault("trace");


    public static  ConfigEntry<String> OPTIMIZER_PLAN_CHANGE_LOG_RULES = buildConf("spark.sql.optimizer.planChangeLog.rules")
            .internal()
            .doc("If this configuration is set, the optimizer will only log plan changes caused by " +
                    "applying the rules specified in this configuration. The value can be a list of rule " +
                    "names separated by comma.")
            .stringConf()
            .createOptional();



    public SQLConf(){

    }

    public static SQLConf getFallbackConf(){
        return fallbackConf.get();
    }

    private static ThreadLocal<SQLConf> existingConf = new ThreadLocal<SQLConf>() {
        @Override
        public SQLConf initialValue(){
            return null;
        }
    };

    private static AtomicReference<Function<Void,SQLConf>> confGetter = new AtomicReference<Function<Void,SQLConf>>(
            (q) ->{
                return fallbackConf.get();
            });

    public int maxToStringFields(){
        //spark related thins
        //getConf(SQLConf.MAX_TO_STRING_FIELDS);
        return 100;
    }


    private static void register(ConfigEntry entry){
        synchronized (sqlConfEntries) {
            ParserUtils.require(!sqlConfEntries.containsKey(entry.getKey()),
                    "Duplicate SQLConfigEntry. "+entry.getKey()+" has been registered");
            sqlConfEntries.put(entry.getKey(), entry);
        }
    }
    public static ConfigBuilder buildConf(String key){
        ConfigBuilder configBuilder = new ConfigBuilder(key);
        return configBuilder.onCreate((entry)->{register(entry); return null;});
    }



    public static SQLConf get() {
        //Lead to spark related things
        if (TaskContext.get() != null) {
            return new ReadOnlySQLConf(TaskContext.get());
        }else{
            //boolean isSchedulerEventLoopThread
            SparkContext sparkContext = SparkContext.getActive();
            boolean isSchedulerEventLoopThread = false;
            if(sparkContext!=null
                    && sparkContext.dagScheduler()!=null
                    && sparkContext.dagScheduler().getEventProcessLoop()!=null
                    && sparkContext.dagScheduler().getEventProcessLoop().getEventThread()!=null){
                isSchedulerEventLoopThread = sparkContext.dagScheduler().getEventProcessLoop().getEventThread().getId() == Thread.currentThread().getId();
            }

            if (isSchedulerEventLoopThread) {
                // DAGScheduler event loop thread does not have an active SparkSession, the `confGetter`
                // will return `fallbackConf` which is unexpected. Here we require the caller to get the
                // conf within `withExistingConf`, otherwise fail the query.
                SQLConf conf = existingConf.get();
                if (conf != null) {
                    return conf;
                } else if (Utils.isTesting()) {
                    throw new RuntimeException("Cannot get SQLConf inside scheduler event loop thread.");
                } else {
                    return confGetter.get().apply((Void)null);
                }
            } else {
                return confGetter.get().apply((Void)null);
            }
        }
    }

    public boolean escapedStringLiterals(){
        return getConf(ESCAPED_STRING_LITERALS);
    }

    public boolean caseSensitiveAnalysis(){
        return getConf(CASE_SENSITIVE);
    }

    public <T> boolean getConf(String entry ){
        return true;
    }

    public <T> T getConf(ConfigEntry<T>entry){
        ParserUtils.require(sqlConfEntries.get(entry.getKey()) == entry, entry+" is not registered");
        return (T)entry.readFrom(reader);
    }

    public int optimizerMaxIterations(){
        return getConf(OPTIMIZER_MAX_ITERATIONS);
    }


    public final Map<String, String> getAllConfs(){
        synchronized (settings){
            return settings;
        }
    }

    protected void setConfWithCheck(String key, String value){
        settings.put(key, value);
    }

    public void setConfString(String key, String value){
        ParserUtils.require(key != null, "key cannot be null");
        ParserUtils.require(value != null, "value cannot be null for key: $key");
        ConfigEntry entry = (ConfigEntry)sqlConfEntries.get(key);
        if (entry != null) {
            // Only verify configs in the SQLConf object
            entry.getValueConverter().apply(value);
        }
        setConfWithCheck(key, value);
    }


    @Override
    public SQLConf clone(){
        SQLConf result = new SQLConf();

        for(Map.Entry<String,String> entry:getAllConfs().entrySet()){
            String k = entry.getKey();
            String v = entry.getValue();
            if(v !=null){
                result.setConfString(k,v);
            }
        }
        return result;
    }

    public SQLConf copy(Pair<ConfigEntry,Object>...entries){
        SQLConf cloned = clone();
        for(Pair<ConfigEntry,Object> pair:entries){
            cloned.setConfString(pair.getKey().getKey(),pair.getValue().toString());
        }
        return cloned;
    }

    public boolean setOpsPrecedenceEnforced(){
        return getConf(LEGACY_SETOPS_PRECEDENCE_ENABLED);
    }

    public boolean supportQuotedRegexColumnName(){
        return getConf(SUPPORT_QUOTED_REGEX_COLUMN_NAME);
    }

    public String optimizerPlanChangeLogLevel(){
        return getConf(OPTIMIZER_PLAN_CHANGE_LOG_LEVEL);
    }
    public String optimizerPlanChangeRules(){
      return  getConf(OPTIMIZER_PLAN_CHANGE_LOG_RULES);
    }

    protected BiFunction<String,String,Boolean> caseInsensitiveResolution = (a,b)->{return  a.equalsIgnoreCase(b);};
    protected BiFunction<String,String,Boolean> caseSensitiveResolution = (a,b)->{return  a==b;};

    public BiFunction<String,String,Boolean> resolver(){
        if (caseSensitiveAnalysis()) {
            return caseSensitiveResolution;
        } else {
            return caseInsensitiveResolution;
        }

    }
}
