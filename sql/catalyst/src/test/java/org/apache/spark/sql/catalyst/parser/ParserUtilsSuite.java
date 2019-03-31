package org.apache.spark.sql.catalyst.parser;

import javafx.util.Pair;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.TokenStream;
import org.apache.spark.SparkFunSuite;
import org.apache.spark.sql.catalyst.trees.TreeNode;
import org.junit.Test;
import org.junit.internal.runners.JUnit4ClassRunner;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.apache.spark.sql.catalyst.trees.TreeNode.Origin;
import org.apache.spark.sql.catalyst.trees.TreeNode.CurrentOrigin;
import java.util.List;
import java.util.function.Function;

import static org.apache.spark.sql.catalyst.parser.ParserUtils.*;
import static org.apache.spark.sql.catalyst.parser.ParserDriver.*;
/**
 * Created by kenya on 2019/3/1.
 */
@SuppressWarnings("ALL")
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:applicationContext-test.xml"})
public class ParserUtilsSuite extends SparkFunSuite {

    SqlBaseParser.SetConfigurationContext setConfContext = buildContext("set example.setting.name=setting.value",(parser)-> {
            return (SqlBaseParser.SetConfigurationContext) parser.statement();
        });

    SqlBaseParser.ShowFunctionsContext showFuncContext = buildContext("show functions foo.bar",(parser)-> {
        return (SqlBaseParser.ShowFunctionsContext)parser.statement();
    });

    SqlBaseParser.DescribeFunctionContext descFuncContext = buildContext("describe function extended bar",(parser)->{
        return (SqlBaseParser.DescribeFunctionContext) parser.statement();
    });

    SqlBaseParser.ShowDatabasesContext showDbsContext = buildContext("show databases like 'identifier_with_wildcards'",(parser) ->{
        return (SqlBaseParser.ShowDatabasesContext) parser.statement();
    });

    SqlBaseParser.StatementContext emptyContext = buildContext("",(parser) ->{
        return parser.statement();
    });


    SqlBaseParser.CreateDatabaseContext createDbContext = buildContext(
            "\n CREATE DATABASE IF NOT EXISTS database_name \n"
                    +" COMMENT 'database_comment' LOCATION '/home/user/db'\n"
            +" WITH DBPROPERTIES ('a'='a', 'b'='b', 'c'='c')".trim(),(parser)-> {
                return (SqlBaseParser.CreateDatabaseContext)parser.statement();
    });

    private <T> T buildContext(String command, Function<SqlBaseParser,T> toResult){
        SqlBaseLexer lexer = new SqlBaseLexer(new UpperCaseCharStream(CharStreams.fromString(command)));
        TokenStream tokenStream = new CommonTokenStream(lexer);
        SqlBaseParser parser = new SqlBaseParser(tokenStream);
        return toResult.apply(parser);
    }


    @Test
    public void testUnescapeSQLString(){
        assert (unescapeSQLString("\"abcdefg\"").equals("abcdefg"));
        assert(unescapeSQLString("'C0FFEE'").equals("C0FFEE"));

        assert(unescapeSQLString("'\0'").equals("\u0000"));
        assert(unescapeSQLString("\"\'\"").equals("\'"));
//        assert(unescapeSQLString("""'\"'""") == "\"")
//        assert(unescapeSQLString(""""\b"""") == "\b")
//        assert(unescapeSQLString("""'\n'""") == "\n")
//        assert(unescapeSQLString(""""\r"""") == "\r")
//        assert(unescapeSQLString("""'\t'""") == "\t")
//        assert(unescapeSQLString(""""\Z"""") == "\u001A")
//        assert(unescapeSQLString("""'\\'""") == "\\")
//        assert(unescapeSQLString(""""\%"""") == "\\%")
//        assert(unescapeSQLString("""'\_'""") == "\\_")

    }

    @Test
    public void testCommand(){
        assert(command(setConfContext).equals("set example.setting.name=setting.value"));
        assert(command(showFuncContext).equals("show functions foo.bar"));
    }

    @Test
    public void testOperationNotAllowed(){
        String errorMessage = "parse.fail.operation.not.allowed.error.message";
        String err=null;
        try{
            operationNotAllowed(errorMessage, showFuncContext);
        }catch (ParseException e){
            err = e.getMessage();
        }
        assert(err.contains("Operation not allowed"));
        assert(err.contains(errorMessage));
    }

    @Test
    public void testCheckDuplicateKeys() {
        List<Pair<String,String>> properties = Seq(new Pair<String,String>("a", "a"), new Pair<String,String>("b", "b"), new Pair<String,String>("c", "c"));

        checkDuplicateKeys(properties, createDbContext);

        List<Pair<String,String>> properties2 = Seq(new Pair<String,String>("a", "a"), new Pair<String,String>("b", "b"), new Pair<String,String>("a", "c"));
        try{
            checkDuplicateKeys(properties2, createDbContext);
        }catch (ParseException e){
            assert (e.getMessage().contains("Found duplicate keys"));
        }
    }


    @Test
    public void testSource(){
        assert(source(setConfContext).equals("set example.setting.name=setting.value"));
        assert(source(showFuncContext).equals("show functions foo.bar"));
        assert(source(descFuncContext).equals("describe function extended bar"));
        assert(source(showDbsContext).equals("show databases like 'identifier_with_wildcards'"));
    }

    @Test
    public void testReminder(){
        assert(remainder(setConfContext).equals(""));
        assert(remainder(showFuncContext).equals(""));
        assert(remainder(descFuncContext).equals(""));
        assert(remainder(showDbsContext).equals(""));

        assert(remainder(setConfContext.SET().getSymbol()).equals(" example.setting.name=setting.value"));
        assert(remainder(showFuncContext.FUNCTIONS().getSymbol()).equals(" foo.bar"));
        assert(remainder(descFuncContext.EXTENDED().getSymbol()).equals( " bar"));
        assert(remainder(showDbsContext.LIKE().getSymbol()).equals(" 'identifier_with_wildcards'"));
    }

    @Test
    public void testString() {
        assert(string(showDbsContext.pattern).equals("identifier_with_wildcards"));
        assert(string(createDbContext.comment).equals("database_comment"));

        assert(string(createDbContext.locationSpec().STRING()).equals("/home/user/db"));
    }

    @Test
    public void testPosition() {
        assert(position(setConfContext.start).equals(new Origin(1, 0)));
        assert(position(showFuncContext.stop).equals(new Origin(1, 19)));
        assert(position(descFuncContext.describeFuncName().start).equals(new Origin(1, 27)));
        assert(position(createDbContext.locationSpec().start).equals(new Origin(3, 28)));
        assert(position(emptyContext.stop).equals(new Origin(null, null)));
    }

    @Test
    public void testValidate(){
        Function<ParserRuleContext,Boolean> f1 = (ctx)->{
            return ctx.children != null && !ctx.children.isEmpty();
        };
        String message = "ParserRuleContext should not be empty.";
        validate(f1.apply(showFuncContext), message, showFuncContext);

        try{
            validate(f1.apply(emptyContext), message, emptyContext);
        }catch (ParseException e){
            assert(e.getMessage().contains(message));
        }

    }


    @Test
    public void testWithOrigin() {
        SqlBaseParser.LocationSpecContext ctx = createDbContext.locationSpec();
        Origin current = CurrentOrigin.get();
        Pair<String,Origin> pair = withOrigin(ctx,(c)->{
            return new Pair<>(string(c.STRING()), CurrentOrigin.get());
        });
        String location = pair.getKey();
        Origin origin = pair.getValue();

        assert(location.equals("/home/user/db"));
        assert(origin.equals(new Origin(3, 28)));
        assert(CurrentOrigin.get().equals(current));
    }

}
