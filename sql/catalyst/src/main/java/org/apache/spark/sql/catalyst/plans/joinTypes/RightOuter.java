package org.apache.spark.sql.catalyst.plans.joinTypes;

/**
 * Created by kenya on 2019/1/21.
 */
public class RightOuter extends JoinType{
    public RightOuter(){
        this.sql = "RIGHT OUTER";
    }

}
