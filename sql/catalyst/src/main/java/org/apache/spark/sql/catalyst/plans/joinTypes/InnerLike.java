package org.apache.spark.sql.catalyst.plans.joinTypes;

import lombok.Data;

/**
 * Created by kenya on 2019/1/21.
 */
@Data
public class InnerLike extends JoinType {
    protected boolean explicitCartesian;

    @Override
    public boolean equals(Object o){
        if(o instanceof InnerLike){
            InnerLike like = (InnerLike)o;
            return explicitCartesian==like.explicitCartesian && super.equals(o);
        }
        return false;
    }
}
