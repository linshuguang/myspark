package org.apache.spark.sql.catalyst.plans.joinTypes;

/**
 * Created by kenya on 2019/1/21.
 */
public class FullOuter extends JoinType {

    public FullOuter(){
        this.sql = "FULL OUTER";
    }

}
