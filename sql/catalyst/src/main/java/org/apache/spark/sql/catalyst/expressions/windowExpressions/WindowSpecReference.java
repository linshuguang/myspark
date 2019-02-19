package org.apache.spark.sql.catalyst.expressions.windowExpressions;

import lombok.Data;

/**
 * Created by kenya on 2019/2/18.
 */
@Data
public class WindowSpecReference implements  WindowSpec{
    String name;

    public WindowSpecReference(String name){
        this.name = name;
    }
}
