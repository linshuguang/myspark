package org.apache.spark.sql.catalyst.parser;

import org.apache.spark.lang.Symbol;
import org.apache.spark.sql.catalyst.analysis.unresolved.MultiAlias;
import org.apache.spark.sql.catalyst.analysis.unresolved.UnresolvedStar;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.PlanTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import static org.apache.spark.sql.catalyst.dsl.expressions.ExpressionConversions.*;
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
            Symbol s){
        assertEqual(sqlCommand,convert(s));
    }

    void assertEqual(
            String sqlCommand,
            Object e){
        if(e instanceof Symbol) {
            assertEqual(sqlCommand, (Symbol)e);
        }else if(e instanceof Expression){
            assertEqual(sqlCommand, (Expression) e);
        }
    }

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

    @Test
    public void testNamedExpressions() {
        DslDriver dslDriver = new DslDriver();
        // No Alias
//        Symbol r0 = new Symbol("a");
//        assertEqual("a", r0);

        //debug
        Object r  = dslDriver.implicit("'a()");

        // Single Alias.
//        Object r1  = dslDriver.expression("'a as \"b\"");
//        assertEqual("a as b", r1);
//        assertEqual("a b", r1);

        // Multi-Alias
        //assertEqual("a as (b, c)", new MultiAlias((Expression)dslDriver.expression("'a"), Arrays.asList("b", "c")));
        //assertEqual("a() (b, c)", new MultiAlias((Expression)dslDriver.expression("'a.function()"), Arrays.asList("b", "c")));
//
                        // Numeric literals without a space between the literal qualifier and the alias, should not be
                        // interpreted as such. An unresolved reference should be returned instead.
                        // TODO add the JIRA-ticket number.
                        assertEqual("1SL", dslDriver.implicit(new Symbol("1SL")));
//
//                        // Aliased star is allowed.
                        //assertEqual("a.* b", UnresolvedStar(Option(Seq("a"))) as 'b)
                System.out.println("ok");

    }

}
