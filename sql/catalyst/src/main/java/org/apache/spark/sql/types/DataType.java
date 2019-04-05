package org.apache.spark.sql.types;

import java.io.Serializable;
import java.util.Locale;

/**
 * Created by kenya on 2019/1/19.
 */
public abstract class DataType extends AbstractDataType implements Serializable {

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

    @Override
    public boolean equals(Object o){
        if(this==o){
            return true;
        }

        if(o!=null || o.getClass()==this.getClass()){
            return true;
        }
        return false;
    }


    public boolean equals(Object l, Object r){

        if(l==r){
            return true;
        }

        if(l==null && r==null){
            return true;
        }
        if(l!=null){
            return l.equals(r);
        }else{
            return r.equals(l);
        }
    }
}
