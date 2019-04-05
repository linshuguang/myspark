package org.apache.spark.sql.catalyst.plans.joinTypes;

/**
 * Created by kenya on 2019/1/21.
 */
public class FullOuter extends JoinType {

    public FullOuter(){
        this.sql = "FULL OUTER";
    }

    @Override
    public boolean equals(Object o){
        if(o instanceof FullOuter){
            return super.equals(o);
        }
        return false;
    }

}
