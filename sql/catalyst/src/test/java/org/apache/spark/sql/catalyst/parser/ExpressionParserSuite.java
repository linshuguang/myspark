package org.apache.spark.sql.catalyst.parser;

import org.apache.spark.sql.catalyst.analysis.unresolved.UnresolvedStar;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.PlanTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by kenya on 2019/3/7.
 */
@SuppressWarnings("ALL")
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:applicationContext-test.xml"})
public class ExpressionParserSuite extends PlanTest {

    @Autowired
    CatalystSqlParser defaultParser;

    void assertEqual(
            String sqlCommand,
            Expression e){
        assertEqual(sqlCommand,e,defaultParser);
    }
    void assertEqual(
            String sqlCommand,
            Expression e,
            ParserInterface parser){
        compareExpressions(parser.parseExpression(sqlCommand), e);
    }

    void intercept(String sqlCommand, String...messages) {
        try {
            defaultParser.parseExpression(sqlCommand);
        } catch (ParseException e) {
            for (String message : messages) {
                assert (e.getMessage().contains(message));
            }
        }
    }

    @Test
    public void testStarExpressions(){
        // Global Star
        assertEqual("*", new UnresolvedStar());

        // Targeted Star
        assertEqual("a.b.*", new UnresolvedStar((Arrays.asList("a", "b"))));
    }

}
