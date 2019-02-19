package java.org.apache.spark.internal.config;

/**
 * Created by kenya on 2019/1/20.
 */
public class ConfigBuilder {
    String key;

    public ConfigBuilder(String key){
        this.key = key;
    }

    public static ConfigBuilder build(String key){
        return new ConfigBuilder(key);
    }


}
