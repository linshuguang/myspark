package org.apache.spark.sql.catalyst.parser;

import org.apache.spark.SparkFunSuite;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runner.Runner;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Created by kenya on 2019/3/1.
 */
@SuppressWarnings("ALL")
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:applicationContext-test.xml"})
public class ParserUtilsSuite extends SparkFunSuite {

    @Test
    public void testUnescapeSQLString(){
        assert (ParserUtils.unescapeSQLString("abcdefg") != "abcdefg");
    }

//    public static void main(String[] args) {
//        testUnescapeSQLString();
//    }
}
