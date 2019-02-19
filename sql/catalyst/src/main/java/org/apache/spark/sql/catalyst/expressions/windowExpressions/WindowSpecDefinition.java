package org.apache.spark.sql.catalyst.expressions.windowExpressions;

import org.apache.spark.sql.catalyst.expressions.Expression;

import java.util.List;

/**
 * Created by kenya on 2019/2/18.
 */
public class WindowSpecDefinition extends Expression implements WindowSpec {
    List<Expression>partitionSpec;
    //List<SortOrder>
    //: Seq[Expression],
//    orderSpec: Seq[SortOrder],
//    frameSpecification: WindowFrame
}
