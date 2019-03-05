package org.apache.spark.sql.internal.config;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Created by kenya on 2019/2/26.
 */
@Data
public class ConfigBuilder {
    String key;

    private boolean _public = true;
    private Function<ConfigEntry,Void> _onCreate = null;
    private String _doc = "";
    private List<String> _alternatives = new ArrayList<>();

    public ConfigBuilder(String key){
        this.key = key;
    }

    public static class  ConfigEntryWithDefaultString<T> extends ConfigEntry {

        String _defaultValue;
        public ConfigEntryWithDefaultString(String key,
                                            List<String> alternatives,
                                            String _defaultValue,
                                            Function<String, T> valueConverter,
                                            Function<T, String> stringConverter,
                                            String doc,
                                            boolean isPublic) {
            super(key, alternatives, valueConverter, stringConverter, doc, isPublic);
            this._defaultValue = _defaultValue;
        }

        public T readFrom(ConfigReader reader){
            T value = (T)readString(reader);
            if(value==null){
                value = (T)reader.substitute(_defaultValue);
            }
            return (T)valueConverter.apply(value);
        }
    }

    public static ConfigBuilder build(String key){
        return new ConfigBuilder(key);
    }

    public ConfigBuilder onCreate(Function<ConfigEntry,Void>callback){
        _onCreate = callback;
        return this;
    }

    public ConfigBuilder internal(){
        _public = false;
        return this;
    }

    public ConfigBuilder doc(String s){
        _doc = s;
        return this;
    }

    public TypedConfigBuilder<Boolean> booleanConf(){
        return new TypedConfigBuilder(this, (s)->{return toBoolean((String)s, key);});
    }

    public TypedConfigBuilder<Integer> intConf(){
        return new TypedConfigBuilder(this, (s)->{
            return (Integer)toNumber((String)s, (ss)->{return Integer.valueOf((String)ss);},key,"int");
        });
        //return new TypedConfigBuilder(this,(s)->{return (Integer)toNumber((String)s, Integer.valueOf((String)s),key,"int")});
    }

    private Boolean toBoolean(String s, String key){
        try {
            return Boolean.valueOf(s.trim());
        } catch (Exception e){
            if(e instanceof IllegalArgumentException) {
                throw new IllegalArgumentException("$key should be boolean, but was $s");
            }else{
                throw new IllegalArgumentException(e.getLocalizedMessage());
            }
        }
    }

    private <T>T toNumber(String s, Function<String,T>converter, String key, String configType){
        try {
            return converter.apply(s.trim());
        } catch(Exception e) {
            if(e instanceof NumberFormatException)
                throw new IllegalArgumentException("$key should be $configType, but was $s");
            else
                throw new IllegalArgumentException("$key should be $configType, but was $s");
        }
    }

    public TypedConfigBuilder<String> stringConf(){
        return new TypedConfigBuilder(this, (s)->{return s;});
    }


}
