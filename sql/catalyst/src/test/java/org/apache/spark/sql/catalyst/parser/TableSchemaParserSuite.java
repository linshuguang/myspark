package org.apache.spark.sql.catalyst.parser;

import org.apache.spark.SparkFunSuite;
import org.apache.spark.sql.types.*;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Arrays;
import java.util.function.Function;

/**
 * Created by kenya on 2019/4/6.
 */
@SuppressWarnings("ALL")
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:applicationContext-test.xml"})
public class TableSchemaParserSuite extends SparkFunSuite {

    private String HIVE_TYPE_STRING = "HIVE_TYPE_STRING";
    @Autowired
    CatalystSqlParser catalystSqlParser;

    StructType parse(String sql){
     return catalystSqlParser.parseTableSchema(sql);
    }

    void assertEqual(Object l,Object r){
        assert l.equals(r);
    }
    void checkTableSchema(String tableSchemaString, DataType expectedDataType){

            assertEqual(parse(tableSchemaString),expectedDataType);

    }
    void intercept(Function<Void,Void> f, Class ec) {
        try {
            f.apply((Void)null);
        } catch (Exception e) {
            if(!e.getClass().isAssignableFrom(ec)){
                throw new RuntimeException("not as expected");
            }
        }
    }
    void assertError(String sql){
        intercept((Void)->{ catalystSqlParser.parseTableSchema(sql); return (Void)null;},ParseException.class);
    }

    @Test
    public void TestTableSchema(){
        checkTableSchema("a int", new StructType().add("a", "int"));
        checkTableSchema("A int", new StructType().add("A", "int"));
        checkTableSchema("a INT", new StructType().add("a", "int"));
        checkTableSchema("`!@#$%.^&*()` string", new StructType().add("!@#$%.^&*()", "string"));
        checkTableSchema("a int, b long", new StructType().add("a", "int").add("b", "long"));
        checkTableSchema("a STRUCT<intType: int, ts:timestamp>",
                new StructType(
                        Arrays.asList(
                        new StructField("a", new StructType(
                                Arrays.asList(new StructField("intType", new IntegerType()),
                                        new StructField("ts", new TimestampType())))))));
        checkTableSchema(
                "a int comment 'test'",
                new StructType().add("a", "int", true, "test"));
    }


    @Test
    public void testNegativeCases() {
        assertError("");
        assertError("a");
        assertError("a INT b long");
        assertError("a INT,, b long");
        assertError("a INT, b long,,");
        assertError("a INT, b long, c int,");
    }


    @Test
    public void testComplexHiveType() {
        String tableSchemaString =" complexStructCol struct< struct:struct<deciMal:DECimal, anotherDecimal:decimAL(5,2)>,MAP:Map<timestamp, varchar(10)>,arrAy:Array<double>,anotherArray:Array<char(9)>>";


        MetadataBuilder builder = new MetadataBuilder();
        builder.putString(HIVE_TYPE_STRING,
                "struct<struct:struct<deciMal:decimal(10,0),anotherDecimal:decimal(5,2)>," +
                        "MAP:map<timestamp,varchar(10)>,arrAy:array<double>,anotherArray:array<char(9)>>");

        StructType expectedDataType =
                new StructType(
                        Arrays.asList(
                            new StructField("complexStructCol",
                                    new StructType(
                                Arrays.asList(
                                        new StructField("struct",
                                                new StructType(
                                                    Arrays.asList(
                                                        new StructField("deciMal", DecimalType.USER_DEFAULT),
                                                        new StructField("anotherDecimal", new DecimalType(5, 2))))) ,
                        new StructField("MAP", new MapType(new TimestampType(), new StringType())),
                        new StructField("arrAy", new ArrayType(new DoubleType())),
                        new StructField("anotherArray", new ArrayType(new StringType())))),
                        true,
                        builder.build())));

        assertEqual(parse(tableSchemaString) , expectedDataType);
    }


}
