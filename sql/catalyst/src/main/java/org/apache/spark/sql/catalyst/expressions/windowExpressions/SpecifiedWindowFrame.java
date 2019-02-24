package org.apache.spark.sql.catalyst.expressions.windowExpressions;

import org.apache.spark.sql.catalyst.expressions.Expression;

/**
 * Created by kenya on 2019/2/22.
 */
public class SpecifiedWindowFrame extends WindowFrame {
    FrameType frameType;
    Expression lower;
    Expression upper;
    public SpecifiedWindowFrame(FrameType frameType,
            Expression lower,
            Expression upper){
        this.frameType = frameType;
        this.lower = lower;
        this.upper = upper;
    }
}
