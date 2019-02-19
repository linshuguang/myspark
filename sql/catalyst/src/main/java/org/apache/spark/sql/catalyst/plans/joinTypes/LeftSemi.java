package org.apache.spark.sql.catalyst.plans.joinTypes;

/**
 * Created by kenya on 2019/1/21.
 */
public class LeftSemi extends JoinType {
    public LeftSemi(){
        this.sql = "LEFT SEMI";
    }
}
