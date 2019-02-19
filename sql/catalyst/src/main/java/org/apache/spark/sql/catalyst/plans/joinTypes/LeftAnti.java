package org.apache.spark.sql.catalyst.plans.joinTypes;

/**
 * Created by kenya on 2019/1/21.
 */
public class LeftAnti extends JoinType {

    public LeftAnti(){
        this.sql = "LEFT ANTI";
    }

}
