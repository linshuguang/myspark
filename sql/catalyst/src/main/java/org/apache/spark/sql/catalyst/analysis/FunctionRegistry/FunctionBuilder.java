package org.apache.spark.sql.catalyst.analysis.FunctionRegistry;

import org.apache.spark.sql.catalyst.expressions.Expression;

import java.util.List;

/**
 * Created by kenya on 2019/1/22.
 */
@FunctionalInterface
public interface FunctionBuilder {
    Expression apply (List<Expression> expressions);
}
