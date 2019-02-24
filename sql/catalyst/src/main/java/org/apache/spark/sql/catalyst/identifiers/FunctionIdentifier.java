package org.apache.spark.sql.catalyst.identifiers;

import lombok.Data;

/**
 * Created by kenya on 2019/1/19.
 */
@Data
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
