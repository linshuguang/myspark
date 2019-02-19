package org.apache.spark.sql.catalyst.util;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.TimeZone;

/**
 * Created by kenya on 2019/1/24.
 */
public class DateTimeUtils {

    private static final long SECONDS_PER_DAY = 60 * 60 * 24L;
    private static final long MILLIS_PER_DAY = SECONDS_PER_DAY * 1000L;

    public static class SQLTimestamp {
        long value;
        public SQLTimestamp(long value){
            this.value = value;
        }
    }

    public static class SQLDate {
        int value;
        public SQLDate(int value){
            this.value = value;
        }
    }

    public static SQLTimestamp fromJavaTimestamp(Timestamp t){
        if (t != null) {
            return new SQLTimestamp(t.getTime() * 1000L + (Long.valueOf(t.getNanos()) / 1000) % 1000L);
        } else {
            return new SQLTimestamp(0L);
        }
    }

    public static TimeZone defaultTimeZone(){
        return TimeZone.getDefault();
    }

    public static SQLDate millisToDays(Long millisUtc, TimeZone timeZone){
        // SPARK-6785: use Math.floor so negative number of days (dates before 1970)
        // will correctly work as input for function toJavaDate(Int)
        long millisLocal = millisUtc + timeZone.getOffset(millisUtc);
        return new SQLDate(Integer.valueOf((int)Math.floor(Double.valueOf(millisLocal) / MILLIS_PER_DAY)));
    }
    public static SQLDate millisToDays(Long millisUtc){
        return millisToDays(millisUtc, defaultTimeZone());
    }
    public static SQLDate fromJavaDate(Date date) {
        return millisToDays(date.getTime());
    }

}
