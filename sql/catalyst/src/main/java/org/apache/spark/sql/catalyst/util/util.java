package org.apache.spark.sql.catalyst.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.internal.SQLConf;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by kenya on 2019/1/20.
 */
public class util {



    private static AtomicBoolean truncationWarningPrinted = new AtomicBoolean(false);


    //first joint point
    public static <T> String truncatedString( List<T> seq,String start,String sep,String end){

        Integer maxNumFields = SQLConf.get().maxToStringFields();

        if (seq.size() > maxNumFields) {
            if (truncationWarningPrinted.compareAndSet(false, true)) {
//                logWarning(
//                        "Truncated the string representation of a plan since it was too large. This " +
//                                s"behavior can be adjusted by setting '${SQLConf.MAX_TO_STRING_FIELDS.key}'.")
            }
            int numFields = Math.max(0, maxNumFields - 1);
            return StringUtils.join(seq.subList(0, numFields), ',');
//            seq.take(numFields).mkString(
//                    start, sep, sep + "... " + (seq.length - numFields) + " more fields" + end)
        } else {
            return StringUtils.join(seq, ',');
        }


    }
}
