package org.apache.spark.sql.catalyst.identifiers;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;

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

    @Override
    public boolean equals(Object o){
        if(o instanceof FunctionIdentifier){
            FunctionIdentifier f = (FunctionIdentifier)o;
            if(!StringUtils.equals(database,f.database)){
                return false;
            }
            return super.equals(o);
        }
        return false;
    }

}
