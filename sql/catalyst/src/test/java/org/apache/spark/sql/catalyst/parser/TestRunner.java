package org.apache.spark.sql.catalyst.parser;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
/**
 * Created by kenya on 2019/3/4.
 */
public class TestRunner {
    public static void main(String[] args) {
        Result result = JUnitCore.runClasses(ParserTest.class);

        for (Failure failure : result.getFailures()) {
            System.out.println(failure.toString());
        }

        System.out.println(result.wasSuccessful());
    }
}
