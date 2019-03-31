package org.apache.spark.sql.catalyst.parser;

import javafx.util.Pair;
import org.apache.spark.sql.catalyst.analysis.AnalysisTest;
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute;
import org.apache.spark.sql.catalyst.analysis.unresolved.UnresolvedAlias;
import org.apache.spark.sql.catalyst.analysis.unresolved.UnresolvedFunction;
import org.apache.spark.sql.catalyst.analysis.unresolved.UnresolvedRelation;
import org.apache.spark.sql.catalyst.analysis.unresolved.UnresolvedStar;
import org.apache.spark.sql.catalyst.expressions.*;
import org.apache.spark.sql.catalyst.expressions.arithmetic.Divide;
import org.apache.spark.sql.catalyst.expressions.arithmetic.UnaryMinus;
import org.apache.spark.sql.catalyst.expressions.grouping.Cube;
import org.apache.spark.sql.catalyst.expressions.grouping.Rollup;
import org.apache.spark.sql.catalyst.expressions.literals.Literal;
import org.apache.spark.sql.catalyst.expressions.namedExpressions.NamedExpression;
import org.apache.spark.sql.catalyst.expressions.predicates.LessThan;
import org.apache.spark.sql.catalyst.expressions.windowExpressions.RowFrame;
import org.apache.spark.sql.catalyst.expressions.windowExpressions.SpecifiedWindowFrame;
import org.apache.spark.sql.catalyst.expressions.windowExpressions.UnspecifiedFrame;
import org.apache.spark.sql.catalyst.expressions.windowExpressions.WindowSpecDefinition;
import org.apache.spark.sql.catalyst.identifiers.FunctionIdentifier;
import org.apache.spark.sql.catalyst.identifiers.TableIdentifier;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias;
import org.apache.spark.sql.catalyst.plans.logical.basicLogicalOperators.*;
import org.apache.spark.sql.types.IntegerType;
import org.junit.Test;
import org.junit.internal.runners.JUnit4ClassRunner;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import sun.rmi.runtime.Log;
import org.apache.spark.sql.catalyst.expressions.namedExpressions.Alias;

import java.lang.reflect.Array;
import java.util.*;
import java.util.function.Function;

/**
 * Created by kenya on 2019/3/26.
 */
@SuppressWarnings("ALL")
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:applicationContext-test.xml"})
public class PlanParserSuite extends AnalysisTest {

    @Autowired
    CatalystSqlParser catalystSqlParser;

    void assertEqual(
            String sqlCommand,
            LogicalPlan plan){
        comparePlans(catalystSqlParser.parsePlan(sqlCommand), plan, false);
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

    private UnresolvedFunction function(String s, Expression...exprs){
        return new UnresolvedFunction(s, Arrays.asList(exprs), false);
    }


    private LogicalPlan limit(LogicalPlan logicalPlan, Expression limitExpr ){
        return Limit.build(limitExpr, logicalPlan);
    }
    private LogicalPlan insertInto(LogicalPlan logicalPlan,String tableName){
                return insertInto(logicalPlan,tableName,false);
    }
    private LogicalPlan insertInto(LogicalPlan logicalPlan,String tableName, boolean overwrite){
       return new InsertIntoTable(
                new UnresolvedRelation(new TableIdentifier(tableName)),
                new HashMap<>(),
                logicalPlan,
                overwrite,
               false
                );
    }


    private LogicalPlan  union(LogicalPlan logicalPlan, LogicalPlan otherPlan){
        return new Union(logicalPlan,otherPlan);
    }

    private LogicalPlan where(LogicalPlan logicalPlan,Expression condition) {
        return new Filter(condition, logicalPlan);
    }
    private LogicalPlan groupBy(LogicalPlan logicalPlan, List<Expression>groupingExprs,List<Expression>aggregateExprs){

        List<NamedExpression>aliasedExprs = new ArrayList<>();
                for(Expression expr:aggregateExprs){
                    if(expr instanceof NamedExpression){
                        aliasedExprs.add((NamedExpression) expr);
                    }else{
                        aliasedExprs.add(new Alias(expr,expr.toString()));
                    }
                }

        return new Aggregate(groupingExprs, aliasedExprs, logicalPlan);
    }

    private LogicalPlan table(String db,String ref) {
        return new UnresolvedRelation(new TableIdentifier(ref,db));
    }

    private LogicalPlan orderBy(LogicalPlan logicalPlan,SortOrder...sortExprs){
        return new Sort(Arrays.asList(sortExprs), true,logicalPlan);
    }

    private LogicalPlan sortBy(LogicalPlan logicalPlan,SortOrder...sortExprs){
        return new Sort(Arrays.asList(sortExprs), true,logicalPlan);
    }

    private LogicalPlan table(String ref) {
        return new UnresolvedRelation(new TableIdentifier(ref));
    }
    private Expression star(String...names){
        if(names.length==0){
            return new UnresolvedStar();
        }else{
            return new UnresolvedStar(Arrays.asList(names));
        }
    }
    private LogicalPlan select(LogicalPlan logicalPlan, Expression...exprs){
        List<NamedExpression> namedExpressions = new ArrayList<>();
        for(Expression expr:exprs){
            if(expr instanceof NamedExpression){
                namedExpressions.add((NamedExpression)expr);
            }else{
                namedExpressions.add(new UnresolvedAlias(expr));
            }
        }
        return new Project(namedExpressions, logicalPlan);
    }

    @Test
    public void testCaseInsensitive() {
        LogicalPlan plan = select(table("a"),star());
        assertEqual("sELEct * FroM a", plan);
        assertEqual("select * fRoM a", plan);
        assertEqual("SELECT * FROM a", plan);
    }


    @Test
    public void testExplain() {
        intercept("EXPLAIN logical SELECT 1", "Unsupported SQL statement");
        intercept("EXPLAIN formatted SELECT 1", "Unsupported SQL statement");
    }


    @Test
    public void testSetOperations() {
        LogicalPlan a = select(table("a"),star());
        LogicalPlan b = select(table("b"),star());

        assertEqual("select * from a union select * from b", new Distinct(new Union(a,b)));
        assertEqual("select * from a union distinct select * from b", new Distinct(new Union(a,b)));
        assertEqual("select * from a union all select * from b", new Union(a,b));
        assertEqual("select * from a except select * from b", new Except(a,b,false));
        assertEqual("select * from a except distinct select * from b", new Except(a,b,false));
        assertEqual("select * from a except all select * from b", new Except(a,b,true));
        assertEqual("select * from a minus select * from b", new Except(a,b,false));
        assertEqual("select * from a minus all select * from b", new Except(a,b,true));
        assertEqual("select * from a minus distinct select * from b", new Except(a,b,false));
        assertEqual("select * from a " +
                "intersect select * from b", new Intersect(a,b,false));
        assertEqual("select * from a intersect distinct select * from b", new Intersect(a,b,false));
        assertEqual("select * from a intersect all select * from b", new Intersect(a,b,true));
    }



    private With cte(LogicalPlan plan, Pair<String, LogicalPlan>...namedPlans) {
        List<Pair<String, SubqueryAlias>> ctes = new ArrayList<>();
        for (Pair<String, LogicalPlan> pair : namedPlans) {
            ctes.add(new Pair<>(pair.getKey(), new SubqueryAlias(pair.getKey(), pair.getValue())));
        }
        return new With(plan, ctes);
    }

    @Test
    public void testCommonTableExpressions() {
        assertEqual(
                "with cte1 as (select * from a) select * from cte1",
                cte(select(table("cte1"),star()), new Pair<>("cte1",select(table("a"),(star())))));
        assertEqual(
                "with cte1 (select 1) select * from cte1",
                cte(select(table("cte1"),star()), new Pair<>("cte1",select(new OneRowRelation(), Literal.build(new Integer(1))))));
        assertEqual(
                "with cte1 (select 1), cte2 as (select * from cte1) select * from cte2",
                cte(    select(table("cte2"),star()),
                        new Pair<>("cte1", select(new OneRowRelation(), Literal.build(new Integer(1)))),
                            new Pair<>("cte2" , select(table("cte1"),star()))));
        intercept(
                "with cte1 (select 1), cte1 as (select 1 from cte1) select * from cte1",
                "Found duplicate keys 'cte1'");
    }


    @Test
    public void testSimpleSelectQuery() {
        assertEqual("select 1", select(new OneRowRelation(),Literal.build(new Integer(1))));
        assertEqual("select a, b", select(new OneRowRelation(), new UnresolvedAttribute("a"), new UnresolvedAttribute("b")));
        assertEqual("select a, b from db.c", select(table("db", "c"),new UnresolvedAttribute("a"), new UnresolvedAttribute("b")));
        assertEqual("select a, b from db.c where x < 1",
                        select(
                                where(
                                        table("db", "c"),
                                        new LessThan(new UnresolvedAttribute("x"),Literal.build(new Integer(1)))),
                                new UnresolvedAttribute("a"),
                                new UnresolvedAttribute("b")));
                assertEqual(
                        "select a, b from db.c having x < 1",
                        where(
                                groupBy(
                                        table("db", "c"),
                                        null,
                                        Arrays.asList(new UnresolvedAttribute("a"),new UnresolvedAttribute("b"))
                                ),
                                new LessThan(new UnresolvedAttribute("x"),Literal.build(new Integer(1)))));
                assertEqual("select distinct a, b from db.c", new Distinct(select(table("db", "c"),new UnresolvedAttribute("a"), new UnresolvedAttribute("b"))));
                assertEqual("select all a, b from db.c", select(table("db", "c"),new UnresolvedAttribute("a"), new UnresolvedAttribute("b")));
                assertEqual("select from tbl", select( new OneRowRelation(),new Alias(new UnresolvedAttribute("from"),"tbl")));
                        assertEqual("select a from 1k.2m", select (table("1k", "2m"),new UnresolvedAttribute("a")));
    }


    @Test
    public void TestRreverseSelectQuery() {
        //assertEqual("from a", table("a"));
//        assertEqual("from a select b, c", select(table("a"),new UnresolvedAttribute("b"),new UnresolvedAttribute("c")));
        assertEqual(
                "from db.a select b, c where d < 1",
                select (where(table("db", "a"), new LessThan(new UnresolvedAttribute("x"),Literal.build(new Integer(1)))),new UnresolvedAttribute("b"),new UnresolvedAttribute("c")));

        assertEqual("from a select distinct b, c", new Distinct(select(table("a"),new UnresolvedAttribute("b"),new UnresolvedAttribute("c"))));
                        assertEqual(
                                "from (from a union all from b) c select *",
                                select(new SubqueryAlias("c",union(table("a"),table("b"))),star()));
    }

    @Test
    public void testMultiSelectQuery() {
        assertEqual(
                "from a select * select * where s < 10",
                union(select(table("a"), star()), select(where(table("a"), new LessThan(new UnresolvedAttribute("s"), Literal.build(new Integer(10)))), star())));
        intercept(
                "from a select * select * from x where a.s < 10",
                "Multi-Insert queries cannot have a FROM clause in their individual SELECT statements");
        assertEqual(
                "from a insert into tbl1 select * insert into tbl2 select * where s < 10",
                union(
                        insertInto(select(table("a"), star()), "tbl1"),
                        insertInto(
                                select(where(table("a"), new LessThan(new UnresolvedAttribute("s"), Literal.build(new Integer(10)))),star()), "tbl2")));
    }


    @Test
    public void testQueryOrganization() {
        // Test all valid combinations of order by/sort by/distribute by/cluster by/limit/windows
        String baseSql = "select * from t";
        LogicalPlan basePlan = select(table("t"),star());
        Map<String,WindowSpecDefinition> ws = new HashMap<>();
        ws.put("w1",new WindowSpecDefinition(new ArrayList<>(), new ArrayList<>(), new UnspecifiedFrame()));
        List<Pair<String,LogicalPlan>>orderSortDistrClusterClauses = new ArrayList<>();
        orderSortDistrClusterClauses.add(new Pair<>("", basePlan));
        orderSortDistrClusterClauses.add(new Pair<>(" order by a, b desc", orderBy(basePlan,new SortOrder(new UnresolvedAttribute("a"),new Ascending()),new SortOrder(new UnresolvedAttribute("b"),new Descending()))));
        orderSortDistrClusterClauses.add(new Pair<>(" sort by a, b desc", sortBy(basePlan,new SortOrder(new UnresolvedAttribute("a"),new Ascending()),new SortOrder(new UnresolvedAttribute("b"),new Descending()))));


        for(Pair<String,LogicalPlan>pair:orderSortDistrClusterClauses) {
            String s1 = pair.getKey();
            LogicalPlan p1 = pair.getValue();
            String s = baseSql + s1;

            assertEqual(s + " limit 10", limit(p1, Literal.build(new Integer(10))));
            assertEqual(s + " window w1 as ()", new WithWindowDefinition(ws, p1));
            assertEqual(s + " window w1 as () limit 10", limit(new WithWindowDefinition(ws, p1), Literal.build(new Integer(10))));
        }

        String msg = "Combination of ORDER BY/SORT BY/DISTRIBUTE BY/CLUSTER BY is not supported";
        intercept(baseSql +" order by a sort by a", msg);
        intercept(baseSql+" cluster by a distribute by a", msg);
        intercept(baseSql+" order by a cluster by a", msg);
        intercept(baseSql+" order by a distribute by a", msg);
    }



    private LogicalPlan insert(
            LogicalPlan plan,
            Map<String,String>partition,
            boolean overwrite,
            boolean ifPartitionNotExists){
        return new InsertIntoTable(table("s"), partition, plan, overwrite, ifPartitionNotExists);
    }

    private LogicalPlan insert(
            LogicalPlan plan,
            Map<String,String>partition
            ){
        return insert(plan,partition,false,false);
    }

    @Test
    public void testInsertInto() {
        String sql = "select * from t";
        LogicalPlan plan = select(table("t"), star());

        // Single inserts
        assertEqual("insert overwrite table s " + sql,
                insert(plan, new HashMap<>(), true, false));
        Map<String, String> map = new HashMap<>();

        map.put("e", "1");
        assertEqual("insert overwrite table s partition (e = 1) if not exists " + sql,
                insert(plan, map, true, true));

        assertEqual("insert into s " + sql,
                insert(plan, new HashMap<>()));

        map.put("e", "1");
        map.put("c", "d");
        assertEqual("insert into table s partition (c = 'd', e = 1) " + sql,
                insert(plan, map));

        // Multi insert
        LogicalPlan plan2 = select(
                where(
                        table("t"),
                        new LessThan(new UnresolvedAttribute("x"), Literal.build(new Integer(5)))), star());
        assertEqual("from t insert into s select * limit 1 insert into u select * where x > 5",
                union(
                        new InsertIntoTable(
                                table("s"), new HashMap<>(), limit(plan, Literal.build(new Integer(1))), false, false),
                        new InsertIntoTable(
                                table("u"), new HashMap<>(), plan2, false, false)));
    }



    @Test
    public void testInsertWithIfNotExists() {
        String sql = "select * from t";
        intercept("insert overwrite table s partition (e = 1, x) if not exists "+sql,
                "Dynamic partitions do not support IF NOT EXISTS. Specified partitions with value: [x]");
        intercept("insert overwrite table s if not exists "+sql,"mismatched input 'if' expecting {'(', 'SELECT', 'FROM', 'VALUES', 'TABLE', 'MAP', 'REDUCE'}");
    }

    @Test
    public void testAggregation() {
        String sql = "select a, b, sum(c) as c from d group by a, b";

//        // Normal
//        assertEqual(sql,
//                groupBy(table("d"),
//                Arrays.asList(new UnresolvedAttribute("a"),new UnresolvedAttribute("b")),
//                Arrays.asList(new UnresolvedAttribute("a"),new UnresolvedAttribute("b"), new Alias(function("sum",new UnresolvedAttribute("c")),"c"))
//                ));
//
//        // Cube
//        assertEqual(sql+" with cube",
//                groupBy(table("d"),
//                        Arrays.asList(new Cube(Arrays.asList(new UnresolvedAttribute("a"),new UnresolvedAttribute("b")))),
//                        Arrays.asList(new UnresolvedAttribute("a"),new UnresolvedAttribute("b"), new Alias(function("sum",new UnresolvedAttribute("c")),"c"))
//                ));
//
//
//        // Rollup
//        assertEqual(sql+" with rollup",
//                groupBy(table("d"),
//                        Arrays.asList(new Rollup(Arrays.asList(new UnresolvedAttribute("a"),new UnresolvedAttribute("b")))),
//                        Arrays.asList(new UnresolvedAttribute("a"),new UnresolvedAttribute("b"), new Alias(function("sum",new UnresolvedAttribute("c")),"c"))
//                ));

        // Grouping Sets
        assertEqual(sql + " grouping sets((a, b), (a), ())",
                new GroupingSets(
                        Arrays.asList(
                                Arrays.asList(new UnresolvedAttribute("a"), new UnresolvedAttribute("b")),
                                Arrays.asList(new UnresolvedAttribute("a")),
                                new ArrayList<>()
                        ),
                        Arrays.asList(new UnresolvedAttribute("a"), new UnresolvedAttribute("b")),
                        table("d"),
                        Arrays.asList(new UnresolvedAttribute("a"), new UnresolvedAttribute("b"), new Alias(function("sum", new UnresolvedAttribute("c")), "c"))
                ));


        intercept("SELECT a, b, count(distinct a, distinct b) as c FROM d GROUP BY a, b", "extraneous input 'b'");


    }


    @Test
    public void testLimit() {
        String sql = "select * from t";
        LogicalPlan plan = select (table("t"),star());
        assertEqual(sql+" limit 10", limit(plan,Literal.build(new Integer(10))));
        assertEqual(sql+" limit cast(9 / 4 as int)", limit(plan, new Cast(new Divide(Literal.build(new Integer(9)), Literal.build(new Integer(4))), new IntegerType())));
    }


    @Test
    public void testWindowSpec() {
        // Note that WindowSpecs are testing in the ExpressionParserSuite
        String sql = "select * from t";
        LogicalPlan plan = select(table("t"),star());
        WindowSpecDefinition spec =
                new WindowSpecDefinition(
                        Arrays.asList(new UnresolvedAttribute("a"), new UnresolvedAttribute("b")),
                        Arrays.asList(
                                new SortOrder(
                                        new UnresolvedAttribute("c"),
                                        new Ascending(),
                                        new NullsFirst()
                                        )),
                        new SpecifiedWindowFrame(new RowFrame(),new UnaryMinus(Literal.build(new Integer(1))), Literal.build(new Integer(1))));

        Map<String ,WindowSpecDefinition>ws1 = new HashMap<>();
        ws1.put("w1" , spec);
        ws1.put("w2" , spec);
        ws1.put("w3" , spec);

        assertEqual(
                sql +  " window w1 as (partition by a, b order by c rows between 1 preceding and 1 following), w2 as w1, w3 as w1"
                ,
        new WithWindowDefinition(ws1, plan));
//
//        // Fail with no reference.
//        intercept(s"$sql window w2 as w1", "Cannot resolve window reference 'w1'")
//
//        // Fail when resolved reference is not a window spec.
//        intercept(
//                s"""$sql
//                        |window w1 as (partition by a, b order by c rows between 1 preceding and 1 following),
//                |       w2 as w1,
//         |       w3 as w2""".stripMargin,
//        "Window reference 'w2' is not a window specification"
//    )
    }


}
