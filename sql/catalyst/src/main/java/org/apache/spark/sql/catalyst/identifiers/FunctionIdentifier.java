package org.apache.spark.sql.catalyst.identifiers;

/**
 * Created by kenya on 2019/1/19.
 */
public class FunctionIdentifier extends IdentifierWithDatabase{
    String database;

    public FunctionIdentifier(String funcName){
        this(funcName,null);
    }

    public FunctionIdentifier(String funcName, String database){
        super(funcName);
        this.database = database;
    }

    @Override
    public String toString(){
        return unquotedString();
    }

}
