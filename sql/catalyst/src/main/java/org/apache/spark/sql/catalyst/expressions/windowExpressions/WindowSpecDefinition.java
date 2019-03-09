package org.apache.spark.sql.catalyst.expressions.windowExpressions;

import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.SortOrder;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by kenya on 2019/2/18.
 */
public class WindowSpecDefinition extends Expression implements WindowSpec {
    List<Expression>partitionSpec;
    List<SortOrder> orderSpec;
    WindowFrame frameSpecification;

    public WindowSpecDefinition(List<Expression>partitionSpec,
            List<SortOrder> orderSpec,
            WindowFrame frameSpecification){
        this.partitionSpec = partitionSpec;
        this.orderSpec = orderSpec;
        this.frameSpecification = frameSpecification;
    }

    @Override
    protected List<Expression> children(){
        List<Expression> list = new ArrayList<>();
        list.addAll(partitionSpec);
        list.addAll(orderSpec);
        list.add(frameSpecification);
        return list;
    }

}
