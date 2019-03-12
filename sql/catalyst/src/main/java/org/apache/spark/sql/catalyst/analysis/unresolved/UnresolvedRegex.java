package org.apache.spark.sql.catalyst.analysis.unresolved;

/**
 * Created by kenya on 2019/3/10.
 */
public class UnresolvedRegex extends Star {
    String regexPattern;
    String table;
    boolean caseSensitive;
    public UnresolvedRegex(String regexPattern,
            String table,
            boolean caseSensitive){
        this.regexPattern = regexPattern;
        this.table = table;
        this.caseSensitive = caseSensitive;
    }
}
