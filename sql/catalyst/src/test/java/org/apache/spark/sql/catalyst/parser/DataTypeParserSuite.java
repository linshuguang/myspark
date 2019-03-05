package org.apache.spark.sql.catalyst.parser;

import org.apache.spark.SparkFunSuite;
import org.apache.spark.sql.types.DataType;
import org.junit.internal.runners.JUnit4ClassRunner;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;

/**
 * Created by kenya on 2019/3/4.
 */
@SuppressWarnings("ALL")
@RunWith(JUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:applicationContext-test.xml"})
public class DataTypeParserSuite extends SparkFunSuite {

    public DataType parse(String sql){
        return CatalystSqlParser.parseDataType(sql);
    }
}
