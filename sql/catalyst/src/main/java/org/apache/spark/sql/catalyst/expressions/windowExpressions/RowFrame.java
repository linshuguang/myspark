package org.apache.spark.sql.catalyst.expressions.windowExpressions;

import lombok.Data;

/**
 * Created by kenya on 2019/2/22.
 */
public class RowFrame implements FrameType {
    @Override
    public boolean equals(Object o){
        if(o instanceof RowFrame){
            return true;
        }
        return false;
    }
}
