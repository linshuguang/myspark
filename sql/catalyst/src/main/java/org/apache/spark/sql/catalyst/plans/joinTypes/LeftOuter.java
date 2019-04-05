package org.apache.spark.sql.catalyst.plans.joinTypes;

/**
 * Created by kenya on 2019/1/21.
 */
public class LeftOuter extends JoinType {

    public LeftOuter(){
        this.sql = "LEFT OUTER";
    }

    @Override
    public boolean equals(Object o){
        if(o instanceof RightOuter){
            return super.equals(o);
        }
        return false;
    }
}
