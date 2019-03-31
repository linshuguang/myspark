package org.apache.spark.sql.catalyst.expressions.windowExpressions;

/**
 * Created by kenya on 2019/2/22.
 */
public class RangeFrame implements FrameType {
    @Override
    public boolean equals(Object o){
        if(o instanceof RangeFrame){
            return true;
        }
        return false;
    }
}
