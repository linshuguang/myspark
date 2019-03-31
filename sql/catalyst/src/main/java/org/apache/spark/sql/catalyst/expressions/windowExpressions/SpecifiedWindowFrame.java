package org.apache.spark.sql.catalyst.expressions.windowExpressions;

import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.parser.ParserUtils;

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

    @Override
    public boolean equals(Object o){
        if(o instanceof SpecifiedWindowFrame){
            SpecifiedWindowFrame s = (SpecifiedWindowFrame)o;
            if(!ParserUtils.equals(frameType,s.frameType)){
                return false;
            }

            if(!ParserUtils.equals(lower,s.lower)){
                return false;
            }
            if(!ParserUtils.equals(upper,s.upper)){
                return false;
            }
            return true;
        }
        return  false;
    }
}
