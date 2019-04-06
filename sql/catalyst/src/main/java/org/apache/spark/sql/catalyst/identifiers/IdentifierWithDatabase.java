package org.apache.spark.sql.catalyst.identifiers;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import javax.xml.soap.SAAJResult;
import java.io.Serializable;

/**
 * Created by kenya on 2019/1/19.
 */
@Data
public abstract class IdentifierWithDatabase implements Serializable{
    String identifier;
    String database;

    public IdentifierWithDatabase(String identifier, String database){
        this.identifier = identifier;
        this.database = database;
    }

    public IdentifierWithDatabase(String identifier){
        this(identifier, null);
    }

    private String quoteIdentifier(String name){
        return name.replace("`", "``");
    }

    public String quotedString(){
        String replacedId = quoteIdentifier(identifier);
        String replacedDb = quoteIdentifier(database);

        if(replacedDb!=null){
            //return s"`${replacedDb.get}`.`$replacedId`"
            return "`"+replacedDb+"`.`"+replacedId+"`";
        }else{
            //return s"`$replacedId`"
            return "`"+replacedId+"`";
        }

    }

    @Override
    public String toString(){
        return quotedString();
    }

    protected String unquotedString(){
        if (database!=null) {
            //s "${database.get}.$identifier"
            return database+"."+identifier;
        }else {
            return identifier;
        }
    }



    @Override
    public boolean equals(Object o){
        if(o instanceof IdentifierWithDatabase){
            IdentifierWithDatabase t = (IdentifierWithDatabase)o;
            return StringUtils.equals(identifier,t.identifier) && StringUtils.equals(database,t.database);
        }
        return false;
    }

}
