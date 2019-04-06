package org.apache.spark.sql.catalyst.parser;

import org.apache.spark.sql.catalyst.analysis.AnalysisTest;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.apache.spark.sql.catalyst.identifiers.TableIdentifier;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

/**
 * Created by kenya on 2019/4/5.
 */
@SuppressWarnings("ALL")
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:applicationContext-test.xml"})
public class TableIdentifierParserSuite extends AnalysisTest {
    @Autowired
    CatalystSqlParser catalystSqlParser;

    List<String > hiveNonReservedKeyword =Arrays.asList("add","admin","after","analyze","archive","asc","before",
            "bucket","buckets","cascade","change","cluster","clustered","clusterstatus","collection",
            "columns","comment","compact","compactions","compute","concatenate","continue","cost",
            "data","day","databases","datetime","dbproperties","deferred","defined","delimited",
            "dependency","desc","directories","directory","disable","distribute",
            "enable","escaped","exclusive","explain","export","fields","file","fileformat","first",
            "format","formatted","functions","hold_ddltime","hour","idxproperties","ignore","index",
            "indexes","inpath","inputdriver","inputformat","items","jar","keys","key_type","last",
            "limit","offset","lines","load","location","lock","locks","logical","long","mapjoin",
            "materialized","metadata","minus","minute","month","msck","noscan","no_drop","nulls",
            "offline","option","outputdriver","outputformat","overwrite","owner","partitioned",
            "partitions","plus","pretty","principals","protection","purge","read","readonly",
            "rebuild","recordreader","recordwriter","reload","rename","repair","replace",
            "replication","restrict","rewrite","role","roles","schemas","second",
            "serde","serdeproperties","server","sets","shared","show","show_database","skewed",
            "sort","sorted","ssl","statistics","stored","streamtable","string","struct","tables",
            "tblproperties","temporary","terminated","tinyint","touch","transactions","unarchive",
            "undo","uniontype","unlock","unset","unsigned","uri","use","utc","utctimestamp",
            "view","while","year","work","transaction","write","isolation","level","snapshot",
            "autocommit","all","any","alter","array","as","authorization","between","bigint",
            "binary","boolean","both","by","create","cube","current_date","current_timestamp",
            "cursor","date","decimal","delete","describe","double","drop","exists","external",
            "false","fetch","float","for","grant","group","grouping","import","in",
            "insert","int","into","is","pivot","lateral","like","local","none","null",
            "of","order","out","outer","partition","percent","procedure","range","reads","revoke",
            "rollup","row","rows","set","smallint","table","timestamp","to","trigger",
            "true","truncate","update","user","values","with","regexp","rlike",
            "bigint","binary","boolean","current_date","current_timestamp","date","double","float",
            "int","smallint","timestamp","at","position","both","leading","trailing","extract");

    List<String> hiveStrictNonReservedKeyword = Arrays.asList("anti", "full", "inner", "left", "semi", "right",
            "natural", "union", "intersect", "except", "database", "on", "join", "cross", "select", "from",
            "where", "having", "from", "to", "table", "with", "not");

    void assertEqual(Object l,Object r){
        assert l.equals(r);
        //comparePlans(catalystSqlParser.parsePlan(sqlCommand), plan, false);
    }
    void intercept(String sqlCommand, String...messages) {
        try {
            catalystSqlParser.parsePlan(sqlCommand);
        } catch (ParseException e) {
            for (String message : messages) {
                assert (e.getMessage().contains(message));
            }
        }
    }

    void intercept(Function<Void,Void>f, Class ec) {
        try {
            f.apply((Void)null);
        } catch (Exception e) {
            if(!e.getClass().isAssignableFrom(ec)){
                throw new RuntimeException("not as expected");
            }
        }
    }
    @Test
    public void testTableIdentifier(){
        // Regular names.
        assertEqual(new TableIdentifier("q"), catalystSqlParser.parseTableIdentifier("q"));
        assertEqual(new TableIdentifier("q", "d"), catalystSqlParser.parseTableIdentifier("d.q"));
        for(String identifier : Arrays.asList("", "d.q.g", "t:", "${some.var.x}", "tab:1")){
            intercept((Void)->{ catalystSqlParser.parseTableIdentifier(identifier); return (Void)null;},ParseException.class);
        }
    }


    @Test
    public void testQuotedIdentifiers() {
        assertEqual(new TableIdentifier("z", "x.y"),catalystSqlParser.parseTableIdentifier("`x.y`.z"));
        assertEqual(new TableIdentifier("y.z", "x") , catalystSqlParser.parseTableIdentifier("x.`y.z`"));
        assertEqual(new TableIdentifier("z", "`x.y`"), catalystSqlParser.parseTableIdentifier("```x.y```.z"));
        assertEqual(new TableIdentifier("`y.z`", "x") ,catalystSqlParser.parseTableIdentifier("x.```y.z```"));
        assertEqual(new TableIdentifier("x.y.z", null) , catalystSqlParser.parseTableIdentifier("`x.y.z`"));
    }



    @Test
    public void testTableIdentifierStrictKeywords() {
        // SQL Keywords.
        for(String keyword:hiveStrictNonReservedKeyword){
            assertEqual(new TableIdentifier(keyword) , catalystSqlParser.parseTableIdentifier(keyword));
            assertEqual(new TableIdentifier(keyword) , catalystSqlParser.parseTableIdentifier("`"+keyword+"`"));
            assertEqual(new TableIdentifier(keyword, "db"), catalystSqlParser.parseTableIdentifier("db.`"+keyword+"`"));
        }
    }

    @Test
    public void testTableIdentifierNonReservedKeywords() {
        // Hive keywords are allowed.
        for(String nonReserved:hiveNonReservedKeyword){
            assertEqual(new TableIdentifier(nonReserved) , catalystSqlParser.parseTableIdentifier(nonReserved));
        }
    }

    @Test
    public void testContainsNumber(){//("SPARK-17364 table identifier - contains number") {
        assertEqual(catalystSqlParser.parseTableIdentifier("123_") ,new TableIdentifier("123_"));
        assertEqual(catalystSqlParser.parseTableIdentifier("1a.123_") ,new TableIdentifier("123_", "1a"));
        // ".123" should not be treated as token of type DECIMAL_VALUE
        assertEqual(catalystSqlParser.parseTableIdentifier("a.123A") , new TableIdentifier("123A", "a"));
        // ".123E3" should not be treated as token of type SCIENTIFIC_DECIMAL_VALUE
        assertEqual(catalystSqlParser.parseTableIdentifier("a.123E3_LIST") ,new TableIdentifier("123E3_LIST", "a"));
        // ".123D" should not be treated as token of type DOUBLE_LITERAL
        assertEqual(catalystSqlParser.parseTableIdentifier("a.123D_LIST") ,new TableIdentifier("123D_LIST", "a"));
        // ".123BD" should not be treated as token of type BIGDECIMAL_LITERAL
        assertEqual(catalystSqlParser.parseTableIdentifier("a.123BD_LIST") ,new TableIdentifier("123BD_LIST", "a"));
    }


    @Test
    public void testContainsBacktick(){//"SPARK-17832 table identifier - contains backtick") {
        TableIdentifier complexName = new TableIdentifier("`weird`table`name", "`d`b`1");
        assertEqual(complexName, catalystSqlParser.parseTableIdentifier("```d``b``1`.```weird``table``name`"));
        assertEqual(complexName ,catalystSqlParser.parseTableIdentifier(complexName.quotedString()));

        intercept((Void)->{ catalystSqlParser.parseTableIdentifier(complexName.quotedString()); return (Void)null;},ParseException.class);
        TableIdentifier complexName2 = new TableIdentifier("x``y", "d``b");
        assertEqual(complexName2,catalystSqlParser.parseTableIdentifier(complexName2.quotedString()));
    }

}
