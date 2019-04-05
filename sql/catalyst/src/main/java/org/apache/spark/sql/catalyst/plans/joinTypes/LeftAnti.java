package org.apache.spark.sql.catalyst.plans.joinTypes;

/**
 * Created by kenya on 2019/1/21.
 */
public class LeftAnti extends JoinType {

    public LeftAnti(){
        this.sql = "LEFT ANTI";
    }

    @Override
    public boolean equals(Object o){
        if(o instanceof LeftAnti){
            return super.equals(o);
        }
        return false;
    }

}
