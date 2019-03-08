package org.apache.spark.sql.catalyst.parser;

import org.apache.spark.sql.catalyst.analysis.CheckAnalysis;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.RunWith;
import org.junit.runner.notification.Failure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Created by kenya on 2019/3/4.
 */
//@SuppressWarnings("ALL")
public class ParserTest {
    public static Logger LOGGER = LoggerFactory.getLogger(ParserTest.class);

    @Autowired
    AnalysisHelper analysisHelper;

    @Test
    public void testUnescape(){
        System.out.println("hello world");
       // assert (ParserUtils.unescapeSQLString("abcdefg") != "abcdefg");
    }

    public static void main(String[] args) {
        Result result = JUnitCore.runClasses(ParserTest.class);

        for (Failure failure : result.getFailures()) {
            System.out.println(failure.toString());
        }

        System.out.println(result.wasSuccessful());
    }


}
