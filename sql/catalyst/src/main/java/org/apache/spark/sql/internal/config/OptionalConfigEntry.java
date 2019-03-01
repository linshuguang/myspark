package org.apache.spark.sql.internal.config;

import java.util.List;
import java.util.function.Function;

/**
 * Created by kenya on 2019/2/27.
 */
public class OptionalConfigEntry<T> extends ConfigEntry {

    public OptionalConfigEntry(
            String key,
            List<String>alternatives,
            Function<String,T> rawValueConverter,
            Function<T, String>rawStringConverter,
            String doc,
            boolean isPublic){
        super(key,alternatives,rawValueConverter,rawStringConverter,doc,isPublic);
    }

    @Override
    public T readFrom(ConfigReader reader){
        //return readString(reader).map(rawValueConverter);
        return null;
    }

}
