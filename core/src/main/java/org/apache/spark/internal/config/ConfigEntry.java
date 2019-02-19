package java.org.apache.spark.internal.config;

import java.util.List;
import java.util.function.Function;

/**
 * Created by kenya on 2019/1/20.
 */
public abstract class ConfigEntry<T> {

    String key;
    List<String> alternatives;
    String valueConverter;
    Function<T, String> stringConverter;
    String doc;
    boolean isPublic;

    public ConfigEntry(String key,
                       List<String> alternatives,
                       String valueConverter,
                       Function<T, String> stringConverter,
                       String doc,
                       boolean isPublic ){
        this.key = key;
        this.alternatives = alternatives;
        this.valueConverter = valueConverter;
        this.stringConverter = stringConverter;
        this.doc = doc;
        this.isPublic = isPublic;
    }




}
