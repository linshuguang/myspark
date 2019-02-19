package org.apache.spark.sql.catalyst.identifiers;

import lombok.Data;

/**
 * Created by kenya on 2019/1/19.
 */
@Data
public abstract class IdentifierWithDatabase {
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

    private String quotedString(){
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

}