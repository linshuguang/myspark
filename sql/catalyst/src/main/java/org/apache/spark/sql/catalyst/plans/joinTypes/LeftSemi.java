package org.apache.spark.sql.catalyst.plans.joinTypes;

/**
 * Created by kenya on 2019/1/21.
 */
public class LeftSemi extends JoinType {
    public LeftSemi(){
        this.sql = "LEFT SEMI";
    }

    @Override
    public boolean equals(Object o){
        if(o instanceof LeftSemi){
            return super.equals(o);
        }
        return false;
    }

}
