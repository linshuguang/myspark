package org.apache.spark.sql.internal;

import java.io.Serializable;

/**
 * Created by kenya on 2019/1/18.
 */
public class SQLConf implements Serializable {

    public static String LEGACY_HAVING_WITHOUT_GROUP_BY_AS_WHERE="spark.sql.legacy.parser.havingWithoutGroupByAsWhere";


    private ThreadLocal<SQLConf> fallbackConf = new ThreadLocal<SQLConf>() {
        @Override
        public SQLConf initialValue(){
            return new SQLConf();
        }
    };

    public int maxToStringFields(){
        //spark related thins
        //getConf(SQLConf.MAX_TO_STRING_FIELDS);
        return 100;
    }


    SQLConf getFallbackConf = fallbackConf.get();

    private ThreadLocal<SQLConf> existingConf = new ThreadLocal<SQLConf>() {
        @Override
        public SQLConf initialValue(){
            return null;
        }
    };

    public static SQLConf get(){
        //confGetter.get();
        return new SQLConf();
    }


    public <T> boolean getConf(String entry ){
        return true;
    }


}
