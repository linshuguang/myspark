package org.apache.spark.sql.catalyst.identifiers;

import lombok.Data;

/**
 * Created by kenya on 2019/1/19.
 */
@Data
public class AliasIdentifier extends  IdentifierWithDatabase {

    public AliasIdentifier(String identifier){
        this(identifier, null);
    }

    public AliasIdentifier(String identifier, String database){
        super(identifier, database);
    }


    @Override
    public boolean equals(Object o){
        if(o instanceof AliasIdentifier){
            return super.equals(o);
        }
        return false;
    }

}
