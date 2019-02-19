package org.apache.spark.sql.types;

import java.util.Locale;

/**
 * Created by kenya on 2019/1/19.
 */
public abstract class DataType extends AbstractDataType {

    /**
     * Enables matching against DataType for expressions:
     * {{{
     *   case Cast(child @ BinaryType(), StringType) =>
     *     ...
     * }}}
     */
    //private[sql] def unapply(e: Expression): Boolean = e.dataType == this


    int defaultSize;

    public String typeName(){
//        this.getClass().getSimpleName().stripSuffix("$").stripSuffix("Type").stripSuffix("UDT")
//                .toLowerCase(Locale.ROOT);
        return this.getClass().getSimpleName();
    }

    //public abstract DataType asNullable();

    public String simpleString(){
        return typeName();
    }

    public String catalogString(){
        return simpleString();
    }

}
