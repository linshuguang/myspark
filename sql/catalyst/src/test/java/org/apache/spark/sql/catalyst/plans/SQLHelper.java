package org.apache.spark.sql.catalyst.plans;

import javafx.util.Pair;
import org.apache.spark.sql.internal.SQLConf;

import java.util.List;
import java.util.function.Function;

/**
 * Created by kenya on 2019/4/5.
 */
public class SQLHelper {

    public static  void withSQLConf(Function<Void,Void>f,List<Pair<String, String>> pairs){
//        SQLConf conf = SQLConf.get();
//        val (keys, values) = pairs.unzip
//        val currentValues = keys.map { key =>
//            if (conf.contains(key)) {
//                Some(conf.getConfString(key))
//            } else {
//                None
//            }
//        }
//        (keys, values).zipped.foreach { (k, v) =>
//            if (SQLConf.staticConfKeys.contains(k)) {
//                throw new AnalysisException(s"Cannot modify the value of a static config: $k")
//            }
//            conf.setConfString(k, v)
//        }
//        try f finally {
//            keys.zip(currentValues).foreach {
//                case (key, Some(value)) => conf.setConfString(key, value)
//                case (key, None) => conf.unsetConf(key)
//            }
//        }
    }
}
