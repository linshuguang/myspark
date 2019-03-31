package org.apache.spark.sql.catalyst.identifiers;

/**
 * Created by kenya on 2019/1/19.
 */
public class TableIdentifier extends IdentifierWithDatabase {

    public TableIdentifier(String table){
        super(table);
    }

    public TableIdentifier(String table, String database){
        super(table, database);
    }




}
