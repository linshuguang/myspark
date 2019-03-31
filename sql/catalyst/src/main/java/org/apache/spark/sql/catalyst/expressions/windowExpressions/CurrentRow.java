package org.apache.spark.sql.catalyst.expressions.windowExpressions;

/**
 * Created by kenya on 2019/2/22.
 */
public class CurrentRow extends SpecialFrameBoundary {
    @Override
    public boolean equals(Object o){
        if(o instanceof CurrentRow){
            return true;
        }
        return false;
    }
}
