package org.apache.spark.sql.internal.config;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by kenya on 2019/2/27.
 */
public class ConfigReader {
    ConfigProvider conf;

    private static String REF_RE = "\\$\\{(?:(\\w+?):)?(\\S+?)\\}";

    public ConfigReader(ConfigProvider conf){
        this.conf = conf;
    }

    public ConfigReader(Map<String, String> conf){
        this(new MapProvider(conf));
    }

    public String get(String key){
      return substitute(conf.get(key));
    }

    public String substitute(String input){
        return substitute(input, new HashSet<>());
    }
    private String substitute(String input, Set<String>usedRefs){
        //TODO
        return input;
    }
//        if (input != null) {
//            ConfigReader.REF_RE.replaceFirst()replaceAllIn(input, { m =>
//                            val prefix = m.group(1)
//                    val name = m.group(2)
//                    val ref = if (prefix == null) name else s"$prefix:$name"
//            require(!usedRefs.contains(ref), s"Circular reference in $input: $ref")
//
//            val replacement = bindings.get(prefix)
//                    .flatMap(getOrDefault(_, name))
//                    .map { v => substitute(v, usedRefs + ref) }
//          .getOrElse(m.matched)
//            Regex.quoteReplacement(replacement)
//      })
//        } else {
//            input
//        }
//    }
}
