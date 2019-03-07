package org.apache.spark.sql.internal.config;

import lombok.Data;
import org.apache.spark.sql.catalyst.parser.ParserUtils;

import java.util.List;
import java.util.function.Function;

/**
 * Created by kenya on 2019/2/26.
 */
@Data
public abstract class ConfigEntry<T> {

    String key;
    List<String> alternatives;
    Function<String,T> valueConverter;
    Function<T, String> stringConverter;
    String doc;
    boolean isPublic;

    public ConfigEntry(String key,
                       List<String> alternatives,
                       Function<String,T> valueConverter,
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

    protected String readString(ConfigReader reader){
        return ParserUtils.foldLeft(alternatives,reader.get(key),(res, nextKey)->{ if(res==null) return reader.get(nextKey); else return res;});
    }

    public abstract T readFrom(ConfigReader reader);

    @Override
    public String toString(){
        return key;
    }

}