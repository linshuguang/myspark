package org.apache.spark.sql.catalyst.parser;

import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.identifiers.TableIdentifier;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;

/**
 * Created by kenya on 2019/1/18.
 */
public interface ParserInterface {

    //@throws[ParseException]("Text cannot be parsed to a LogicalPlan")
    //LogicalPlan parsePlan(String sqlText);
    DataType parseDataType(String sqlText);
    Expression parseExpression(String sqlText);
    LogicalPlan parsePlan(String sqlText);
    TableIdentifier parseTableIdentifier(String sqlText);
    StructType parseTableSchema(String sqlText);
}
