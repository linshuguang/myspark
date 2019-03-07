package org.apache.spark.sql.internal.config;

import java.util.List;
import java.util.function.Function;

/**
 * Created by kenya on 2019/3/6.
 */
public class ConfigEntryWithDefault<T> extends ConfigEntry {
    T _defaultValue;

    public ConfigEntryWithDefault(String key,
            List<String> alternatives,
            T _defaultValue,
            Function<String,T> valueConverter,
            Function<T,String> stringConverter,
            String doc,
            boolean isPublic){
        super(key, alternatives, valueConverter, stringConverter, doc, isPublic);
        this._defaultValue= _defaultValue;
    }

    @Override
    public T  readFrom(ConfigReader reader){
        String str = readString(reader);
        T t = null;
        if(str!=null) {
            t = (T) valueConverter.apply(str);
        }
        if(t==null){
            return _defaultValue;
        }else{
            return t;
        }
    }


}
