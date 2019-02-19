package org.apache.spark.sql.catalyst.plans.joinTypes;

/**
 * Created by kenya on 2019/1/21.
 */
public class Cross extends InnerLike {

    public Cross(){
        this.explicitCartesian = true;
        this.sql = "CROSS";
    }
}
