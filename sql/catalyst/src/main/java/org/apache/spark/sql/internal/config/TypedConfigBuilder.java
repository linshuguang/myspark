package org.apache.spark.sql.internal.config;

import java.util.function.Function;

/**
 * Created by kenya on 2019/2/27.
 */
public class TypedConfigBuilder<T> {

    ConfigBuilder parent;
    Function<String,T> converter;
    Function<T,String> stringConverter;

    public TypedConfigBuilder(ConfigBuilder parent,
            Function<String,T> converter,
            Function<T,String> stringConverter){
        this.parent = parent;
        this.converter = converter;
        this.stringConverter = stringConverter;
    }

    public TypedConfigBuilder(ConfigBuilder parent,
                              Function<String,T> converter){
        this(parent, converter, (s)->{ return s.toString();});
    }

    public ConfigEntry<T> createWithDefault(T def){
        // Treat "String" as a special case, so that both createWithDefault and createWithDefaultString
        // behave the same w.r.t. variable expansion of default values.
        if (def instanceof String) {
            return createWithDefaultString((String)def);
        } else {//TODO
//            T transformedDefault = converter.apply(stringConverter.apply(def));
//                val entry = new ConfigEntryWithDefault[T](parent.key, parent._alternatives,
//                        transformedDefault, converter, stringConverter, parent._doc, parent._public)
//                parent._onCreate.foreach(_(entry))
//                entry
        }
        return null;
    }


    public <T>ConfigEntry<T> createWithDefaultString(String def){
        ConfigBuilder.ConfigEntryWithDefaultString<T> entry = new ConfigBuilder.ConfigEntryWithDefaultString(parent.getKey(), parent.get_alternatives(), def,
        converter, stringConverter, parent.get_doc(), parent.is_public());
        parent.get_onCreate().apply(entry);
        return entry;
    }


    public <T>OptionalConfigEntry<T> createOptional(){
        OptionalConfigEntry<T> entry = new OptionalConfigEntry(parent.getKey(), parent.get_alternatives(), converter,
                stringConverter, parent.get_doc(), parent.is_public());
        parent.get_onCreate().apply(entry);
        return entry;
    }
}
