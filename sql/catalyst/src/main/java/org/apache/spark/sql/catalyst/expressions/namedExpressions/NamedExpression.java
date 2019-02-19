package org.apache.spark.sql.catalyst.expressions.namedExpressions;

import lombok.Data;
import org.apache.spark.sql.catalyst.expressions.Expression;

/**
 * Created by kenya on 2019/1/30.
 */
@Data
public abstract class NamedExpression extends Expression {
    String name;
}
