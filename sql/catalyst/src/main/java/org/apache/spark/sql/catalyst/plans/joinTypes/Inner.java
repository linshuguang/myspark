package org.apache.spark.sql.catalyst.plans.joinTypes;

/**
 * Created by kenya on 2019/1/21.
 */
public class Inner extends InnerLike {

    public Inner(){
        this.explicitCartesian = false;
        this.sql = "INNER";
    }

    @Override
    public boolean equals(Object o){
        if(o instanceof Inner){
            return super.equals(o);
        }
        return false;
    }


}
