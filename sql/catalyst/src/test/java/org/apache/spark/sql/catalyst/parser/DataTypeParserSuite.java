package org.apache.spark.sql.catalyst.parser;

import org.apache.spark.SparkFunSuite;
import org.apache.spark.sql.types.*;
import org.apache.spark.util.BeanLoader;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Created by kenya on 2019/3/4.
 */
@SuppressWarnings("ALL")
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:applicationContext-test.xml"})
public class DataTypeParserSuite extends SparkFunSuite {

    @Autowired
    CatalystSqlParser catalystSqlParser;

    public DataType parse(String sql){
        return catalystSqlParser.parseDataType(sql);
    }

    protected  void checkDataType(String dataTypeString, DataType expectedDataType){
        DataType dataType = parse(dataTypeString);
        assert(parse(dataTypeString).equals(expectedDataType));
    }

    protected  ParseException intercept(String sql){
        try {
            catalystSqlParser.parseDataType(sql);
        }catch (ParseException e){
            return e;
        }
        return null;
    }

    protected void unsupported(String dataTypeString){
            intercept(dataTypeString);
    }


    @Test
    public void testPrimitiveDataType() {
        checkDataType("int", new IntegerType());
        checkDataType("integer", new IntegerType());
        checkDataType("BooLean", new BooleanType());
        checkDataType("tinYint", new ByteType());
        checkDataType("smallINT", new ShortType());
        checkDataType("INT", new IntegerType());
        checkDataType("INTEGER", new IntegerType());
        checkDataType("bigint", new LongType());
        checkDataType("float", new FloatType());
        checkDataType("dOUBle", new DoubleType());
        checkDataType("decimal(10, 5)", new DecimalType(10, 5));
        checkDataType("decimal", DecimalType.USER_DEFAULT);
        checkDataType("DATE", new DateType());
        checkDataType("timestamp", new TimestampType());
        checkDataType("string", new StringType());
        checkDataType("ChaR(5)", new StringType());
        checkDataType("varchAr(20)", new StringType());
        checkDataType("cHaR(27)", new StringType());
        checkDataType("BINARY", new BinaryType());

    }

    @Test
    public void testArrayType() {
//        checkDataType("array<doublE>", new ArrayType(new DoubleType(), true));
//        checkDataType("Array<map<int, tinYint>>", new ArrayType(new MapType(new IntegerType(), new ByteType(), true), true));
        checkDataType(
                "array<struct<tinYint:tinyint>>",
                new ArrayType(new StructType(new StructField("tinYint", new ByteType(), true)), true)
        );
        checkDataType("MAP<int, STRING>", new MapType(new IntegerType(), new StringType(), true));
        checkDataType("MAp<int, ARRAY<double>>", new MapType(new IntegerType(), new ArrayType(new DoubleType()), true));
        checkDataType(
                "MAP<int, struct<varchar:string>>",
                new MapType(new IntegerType(), new StructType(new StructField("varchar", new StringType(), true)), true)
        );
    }


    @Test
    public void TestStructType() {
//        checkDataType(
//                "struct<intType: int, ts:timestamp>",
//                new StructType(
//                        new StructField("intType", new IntegerType(), true),
//                        new StructField("ts", new TimestampType(), true))
//        );
//        // It is fine to use the data type string as the column name.
//        checkDataType(
//                "Struct<int: int, timestamp:timestamp>",
//                new StructType(
//                        new StructField("int", new IntegerType(), true),
//                        new StructField("timestamp", new TimestampType(), true))
//        );
//        checkDataType(
//                " \nstruct<"
//                        + "       \n  struct:struct<deciMal:DECimal, anotherDecimal:decimAL(5,2)>,"
//                        + "\n  MAP:Map<timestamp, varchar(10)>,"
//                        + "\n  arrAy:Array<double>,"
//                        + "\n  anotherArray:Array<char(9)>>"
//                ,
//                new StructType(
//                        new StructField("struct",
//                                new StructType(
//                                        new StructField("deciMal", DecimalType.USER_DEFAULT, true),
//                                        new StructField("anotherDecimal", new DecimalType(5, 2), true)), true),
//                        new StructField("MAP", new MapType(new TimestampType(), new StringType()), true),
//                        new StructField("arrAy", new ArrayType(new DoubleType(), true), true),
//                        new StructField("anotherArray", new ArrayType(new StringType(), true), true))
//        );
//        // Use backticks to quote column names having special characters.
//        checkDataType(
//                "struct<`x+y`:int, `!@#$%^&*()`:string, `1_2.345<>:\"`:varchar(20)>",
//                new StructType(
//                        new StructField("x+y", new IntegerType(), true),
//                        new StructField("!@#$%^&*()", new StringType(), true),
//                        new StructField("1_2.345<>:\"", new StringType(), true))
//        );
//        // Empty struct.
//        checkDataType("strUCt<>", new StructType());


        // DataType parser accepts certain reserved keywords.
        checkDataType(
                "Struct<TABLE: string, DATE:boolean>",
                new StructType(
                        new StructField("TABLE", new StringType(), true),
                        new StructField("DATE", new BooleanType(), true))
        );

        // Use SQL keywords.
        checkDataType("struct<end: long, select: int, from: string>",
                (new StructType()).add("end", new LongType()).add("select", new IntegerType()).add("from", new StringType()));

        // DataType parser accepts comments.
        checkDataType("Struct<x: INT, y: STRING COMMENT 'test'>",
                (new StructType()).add("x", new IntegerType()).add("y", new StringType(), true, "test"));
    }

    @Test
    public void TestUnsupport(){
        unsupported("it is not a data type");
        unsupported("struct<x+y: int, 1.1:timestamp>");
        unsupported("struct<x: int");
        unsupported("struct<x int, y string>");
    }

}
