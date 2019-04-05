package org.apache.spark.sql.catalyst.parser;

import javafx.util.Pair;
import org.apache.spark.sql.catalyst.analysis.AnalysisTest;
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute;
import org.apache.spark.sql.catalyst.analysis.unresolved.*;
import org.apache.spark.sql.catalyst.expressions.*;
import org.apache.spark.sql.catalyst.expressions.arithmetic.Divide;
import org.apache.spark.sql.catalyst.expressions.arithmetic.UnaryMinus;
import org.apache.spark.sql.catalyst.expressions.complexTypeCreator.CreateStruct;
import org.apache.spark.sql.catalyst.expressions.grouping.Cube;
import org.apache.spark.sql.catalyst.expressions.grouping.Rollup;
import org.apache.spark.sql.catalyst.expressions.literals.Literal;
import org.apache.spark.sql.catalyst.expressions.namedExpressions.NamedExpression;
import org.apache.spark.sql.catalyst.expressions.predicates.*;
import org.apache.spark.sql.catalyst.expressions.subquery.ScalarSubquery;
import org.apache.spark.sql.catalyst.expressions.windowExpressions.RowFrame;
import org.apache.spark.sql.catalyst.expressions.windowExpressions.SpecifiedWindowFrame;
import org.apache.spark.sql.catalyst.expressions.windowExpressions.UnspecifiedFrame;
import org.apache.spark.sql.catalyst.expressions.windowExpressions.WindowSpecDefinition;
import org.apache.spark.sql.catalyst.identifiers.FunctionIdentifier;
import org.apache.spark.sql.catalyst.identifiers.TableIdentifier;
import org.apache.spark.sql.catalyst.plans.joinTypes.*;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias;
import org.apache.spark.sql.catalyst.plans.logical.basicLogicalOperators.*;
import org.apache.spark.sql.catalyst.plans.logical.hints.UnresolvedHint;
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
import java.util.function.BiFunction;
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

    private LogicalPlan intersect(LogicalPlan logicalPlan,LogicalPlan otherPlan,boolean isAll){
        return new Intersect(logicalPlan, otherPlan, isAll);
    }
    private LogicalPlan except(LogicalPlan logicalPlan,LogicalPlan otherPlan,boolean isAll){
        return new Except(logicalPlan, otherPlan, isAll);
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

    private LogicalPlan generate(LogicalPlan logicalPlan,Generator generator){
        return generate(logicalPlan, generator, new ArrayList<>(),false,null,new ArrayList<>());
    }
    private LogicalPlan generate(
            LogicalPlan logicalPlan,
            Generator generator,
            List<Integer>unrequiredChildIndex,
            boolean outer,
            String alias,
            List<String >outputNames){

        List<Attribute> attributes = new ArrayList<>();
        for(String name: outputNames){
            attributes.add(new UnresolvedAttribute(name));
        }
        return new Generate(generator, unrequiredChildIndex, outer,
                alias, attributes, logicalPlan);
    }
    private LogicalPlan as(LogicalPlan logicalPlan, String alias){
        return new SubqueryAlias(alias, logicalPlan);
    }

    private LogicalPlan join(
            LogicalPlan logicalPlan,
            LogicalPlan otherPlan
            ){
        return join(logicalPlan,otherPlan, new Inner(),null);
    }
    private LogicalPlan join(
            LogicalPlan logicalPlan,
            LogicalPlan otherPlan,
            JoinType joinType
    ){
        return join(logicalPlan,otherPlan, joinType,null);
    }

    private LogicalPlan join(
            LogicalPlan logicalPlan,
            LogicalPlan otherPlan,
            JoinType joinType,
            Expression condition){
        return new Join(logicalPlan, otherPlan, joinType, condition);
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

//    private LogicalPlan insertInto(LogicalPlan logicalPlan, String tableName, boolean overwrite) {
//
//
//        return new InsertIntoTable(
//                new UnresolvedRelation(new TableIdentifier(tableName)),
//                new HashMap<>(), logicalPlan, overwrite, false);
//    }

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

        // Fail with no reference.
        intercept(sql+" window w2 as w1", "Cannot resolve window reference 'w1'");

        // Fail when resolved reference is not a window spec.
        intercept(
                sql+" window w1 as (partition by a, b order by c rows between 1 preceding and 1 following), w2 as w1,  w3 as w2",
        "Window reference 'w2' is not a window specification"
        );
    }





    @Test
    public void testLateralView() {
        UnresolvedGenerator explode = new UnresolvedGenerator(new FunctionIdentifier("explode"), Arrays.asList(new UnresolvedAttribute("x")));
        UnresolvedGenerator jsonTuple = new UnresolvedGenerator(new FunctionIdentifier("json_tuple"), Arrays.asList(new UnresolvedAttribute("x"), new UnresolvedAttribute("y")));

        // Single lateral view
        assertEqual(
                "select * from t lateral view explode(x) expl as x",
                select(generate(table("t"), explode, new ArrayList<>(), false, "expl", Arrays.asList("x")), star()));

        // Multiple lateral views
        assertEqual(
                "select * from t lateral view explode(x) expl lateral view outer json_tuple(x, y) jtup q, z",
                select(generate(generate(table("t"), explode, new ArrayList<>(), false, "expl", new ArrayList<>())
                        , jsonTuple, new ArrayList<>(), true, "jtup", Arrays.asList("q", "z")), star()));


        // Multi-Insert lateral views.
        LogicalPlan from = generate(table("t1"), explode, new ArrayList<>(), false, "expl", Arrays.asList("x"));
        assertEqual(
                "from t1 lateral view explode(x) expl as x insert into t2 select * lateral view json_tuple(x, y) jtup q, z insert into t3 select * where s < 10",

                new Union(
                        insertInto(select(generate(from, jsonTuple, new ArrayList<>(), false, "jtup", Arrays.asList("q", "z")), star()), "t2"),
                        insertInto(select(where(from, new LessThan(new UnresolvedAttribute("s"), Literal.build(new Integer(10)))), star()), "t3"))
        );

        // Unresolved generator.
        LogicalPlan expected = select(generate(
                table("t"),
                new UnresolvedGenerator(new FunctionIdentifier("posexplode"), Arrays.asList(new UnresolvedAttribute("x"))),
                new ArrayList<>(),
                false,
                "posexpl",
                Arrays.asList("x")),star());

        assertEqual(
                "select * from t lateral view posexplode(x) posexpl as x, y",
                expected);

        intercept(
                "select * from t lateral view explode(x) expl pivot (   sum(x)   FOR y IN ('a', 'b') )",

                "LATERAL cannot be used together with PIVOT in FROM clause");
    }



    private void testUnconditionalJoin(String sql, JoinType jt) {
        assertEqual(
                "select * from t as tt " + sql + " u",
                select(join(as(table("t"), "tt"), table("u"), jt, null), star()));
    }
    private void testConditionalJoin(String sql, JoinType jt){
        assertEqual(
                "select * from t "+sql+" u as uu on a = b",
                select(join(table("t"),as(table("u"),"uu"), jt, new EqualTo(new UnresolvedAttribute("a"),new UnresolvedAttribute("b"))),star()));
    }
    private void testNaturalJoin(String sql, JoinType jt){
        assertEqual(
                "select * from t tt natural "+sql+" u as uu",
                select(join(as(table("t"),"tt"),as(table("u"),"uu"), new NaturalJoin(jt), null),star()));
    }
    private void testUsingJoin(String sql,JoinType jt) {
        assertEqual(
                "select * from t " + sql + " u using(a, b)",
                select(join(table("t"), table("u"), new UsingJoin(jt, Arrays.asList("a", "b")), null), star()));
    }

    private void test(String sql, JoinType jt, List<BiFunction<String,JoinType,Void>> tests){
        for(BiFunction<String,JoinType,Void> test:tests){
            test.apply(sql,jt);
        }
    }



    @Test
    public void testJoins() {


        List<BiFunction<String,JoinType,Void>> testAll =
                Arrays.asList(
                        (s,j)->{testUnconditionalJoin(s,j);return (Void)null;},
                        (s,j)->{testConditionalJoin(s,j);return (Void)null;},
                        (s,j)->{testNaturalJoin(s,j);return (Void)null;},
                        (s,j)->{testUsingJoin(s,j);return (Void)null;}
                        );
        List<BiFunction<String,JoinType,Void>> testExistence =
                Arrays.asList(
                        (s,j)->{testUnconditionalJoin(s,j);return (Void)null;},
                        (s,j)->{testConditionalJoin(s,j);return (Void)null;},
                        (s,j)->{testUsingJoin(s,j);return (Void)null;}
                );


        test("cross join", new Cross(),
                Arrays.asList(
                    (s,j)->{testUnconditionalJoin(s,j);return (Void)null;}
                    )
        );
        test(",", new Inner(),
                Arrays.asList(
                        (s,j)->{testUnconditionalJoin(s,j);return (Void)null;}
                )
        );
        test("join", new Inner(), testAll);
        test("inner join", new Inner(), testAll);
        test("left join", new LeftOuter(), testAll);
        test("left outer join", new LeftOuter(), testAll);
        test("right join", new RightOuter(), testAll);
        test("right outer join", new RightOuter(), testAll);
        test("full join", new FullOuter(), testAll);
        test("full outer join", new FullOuter(), testAll);
        test("left semi join", new LeftSemi(), testExistence);
        test("left anti join",new  LeftAnti(), testExistence);
        test("anti join", new LeftAnti(), testExistence);

        // Test natural cross join
        intercept("select * from a natural cross join b");

        // Test natural join with a condition
        intercept("select * from a natural join b on a.id = b.id");

        // Test multiple consecutive joins
        assertEqual(
                "select * from a join b join c right join d",
                select(join(join(join(table("a"),table("b")),table("c")),table("d"), new RightOuter()),star()));

        // SPARK-17296
        assertEqual(
                "select * from t1 cross join t2 join t3 on t3.id = t1.id join t4 on t4.id = t1.id",
                select(join(join(join(table("t1"),table("t2"), new Cross()),
                        table("t3"), new Inner(), new EqualTo(new UnresolvedAttribute("t3.id"), new UnresolvedAttribute("t1.id"))),
                        table("t4"), new Inner(), new EqualTo(new UnresolvedAttribute("t4.id"), new UnresolvedAttribute("t1.id")))
                        ,star()));

        // Test multiple on clauses.
        intercept("select * from t1 inner join t2 inner join t3 on col3 = col2 on col3 = col1");

        // Parenthesis
        assertEqual(
                "select * from t1 inner join (t2 inner join t3 on col3 = col2) on col3 = col1",
                select(join(join(table("t1"),table("t2")),
                                table("t3"), new Inner(), new EqualTo(new UnresolvedAttribute("col3"), new UnresolvedAttribute("col2")))
                        ,star()));
        assertEqual(
                "select * from t1 inner join (t2 inner join t3) on col3 = col2",
                select(
                        join(table("t1"),
                        join(table("t2"),table("t3"), new Inner(), null), new Inner(),new EqualTo(new UnresolvedAttribute("col3"), new UnresolvedAttribute("col2")))
                        ,star()
                        ));
        assertEqual(
                "select * from t1 inner join (t2 inner join t3 on col3 = col2)",
                select(
                        join(table("t1"),
                                join(table("t2"),table("t3"), new Inner(), new EqualTo(new UnresolvedAttribute("col3"), new UnresolvedAttribute("col2"))),
                                new Inner(), null),
                        star()));

        // Implicit joins.
        assertEqual(
                "select * from t1, t3 join t2 on t1.col1 = t2.col2",
                select( join(
                        join(table("t1"),table("t3"))
                        ,table("t2"), new Inner(), new EqualTo(new UnresolvedAttribute("t1.col1"), new UnresolvedAttribute("t2.col2"))),star()));
    }


    @Test
    public void testSampledRelations() {
        String sql = "select * from t";
        assertEqual(sql+" tablesample(100 rows)",
                select(limit(table("t"),Literal.build(new Integer(100))),star()));
        assertEqual(sql+" tablesample(43 percent) as x",
                select(new Sample(0.0, 0.43, false, 10L, as(table("t"),"x")),star()));
        assertEqual(sql+" tablesample(bucket 4 out of 10) as x",
                select(new Sample(0.0, 0.4,false, 10L,as(table("t"),"x")),star()));
        intercept(sql+" tablesample(bucket 4 out of 10 on x) as x",
                "TABLESAMPLE(BUCKET x OUT OF y ON colname) is not supported");
//        intercept(sql+" tablesample(bucket 11 out of 10) as x",
//                "Sampling fraction (${11.0/10.0}) must be on interval [0, 1]");
        intercept("SELECT * FROM parquet_t0 TABLESAMPLE(300M) s",
                "TABLESAMPLE(byteLengthLiteral) is not supported");
        intercept("SELECT * FROM parquet_t0 TABLESAMPLE(BUCKET 3 OUT OF 32 ON rand()) s",
                "TABLESAMPLE(BUCKET x OUT OF y ON function) is not supported");
    }


    @Test
    public void testSubQuery() {
        LogicalPlan plan = select(table("t0"),new UnresolvedAttribute("id"));
                assertEqual("select id from (t0)", plan);
                assertEqual("select id from ((((((t0))))))", plan);
                assertEqual(
                        "(select * from t1) union distinct (select * from t2)",
                        new Distinct(union(select(table("t1"),star()),select(table("t2"),star()))));
                assertEqual(
                        "select * from ((select * from t1) union (select * from t2)) t",
                        select(as(new Distinct(
                                union(select(table("t1"),star()),select(table("t2"),star()))),"t"),star()));
                assertEqual(
                        "select  id from (((select id from t0)        union all      (select  id from t0))       union all     (select id from t0)) as u_1",
                        select(as(union(union(plan,plan),plan),"u_1"), new UnresolvedAttribute("id")));
    }

    @Test
    public void testScalarSubQuery() {
        assertEqual(
                "select (select max(b) from s) ss from t",
                select(table("t"),new Alias(new ScalarSubquery(select(table("s"),function("max",new UnresolvedAttribute("b")))),"ss")));

        assertEqual(
                "select * from t where a = (select b from s)",
                select(
                        where(
                                table("t"),
                                new EqualTo(new UnresolvedAttribute("a"),new ScalarSubquery(select(table("s"), new UnresolvedAttribute("b"))))),star()));


        assertEqual(
                "select g from t group by g having a > (select b from s)",
                where(groupBy(table("t"),
                        Arrays.asList(new UnresolvedAttribute("g")),
                        Arrays.asList(new UnresolvedAttribute("g"))),
                        new GreaterThan(new UnresolvedAttribute("a"),new ScalarSubquery(select(table("s"),new UnresolvedAttribute("b"))))));

    }

    @Test
    public void testTableReference() {
        assertEqual("table t", table("t"));
        assertEqual("table d.t", table("d", "t"));
    }

    @Test
    public void testTableValuedFunction() {
        assertEqual(
                "select * from range(2)",
                select(new UnresolvedTableValuedFunction("range", Arrays.asList(Literal.build(new Integer(2))), new ArrayList<>()),star()));
    }


    @Test
    public void testRangeAsAlias(){ //("SPARK-20311 range(N) as alias")
        assertEqual(
                "SELECT * FROM range(10) AS t",
                select(new SubqueryAlias("t", new UnresolvedTableValuedFunction("range", Arrays.asList(Literal.build(new Integer(10))),new ArrayList<>())),star()));

        assertEqual(
                "SELECT * FROM range(7) AS t(a)",
                select(new SubqueryAlias("t", new UnresolvedTableValuedFunction("range", Arrays.asList(Literal.build(new Integer(7))),Arrays.asList("a"))),star()));
    }


    @Test
    public void testAliasInFrom(){//("SPARK-20841 Support table column aliases in FROM clause") {
        assertEqual(
                "SELECT * FROM testData AS t(col1, col2)",
                select(new UnresolvedSubqueryColumnAliases(
                        Arrays.asList("col1", "col2"),
                        new SubqueryAlias("t", new UnresolvedRelation(new TableIdentifier("testData")))
                ),star()));
    }

    @Test
    public void testSubQueryInFrom(){// ("SPARK-20962 Support subquery column aliases in FROM clause") {
        assertEqual(
                "SELECT * FROM (SELECT a AS x, b AS y FROM t) t(col1, col2)",
                select(new UnresolvedSubqueryColumnAliases(
                        Arrays.asList("col1", "col2"),
                        new SubqueryAlias(
                                "t",
                                select(
                                        new UnresolvedRelation(new TableIdentifier("t")),
                                        new Alias(new UnresolvedAttribute("a"),"x"),
                                                new Alias(new UnresolvedAttribute("b"),"y")))
                ),star()));
    }

    @Test
    public void testAliasForJoinRelation(){//("SPARK-20963 Support aliases for join relations in FROM clause") {
        LogicalPlan src1 = as(new UnresolvedRelation(new TableIdentifier("src1")),"s1");
        LogicalPlan src2 = as(new UnresolvedRelation(new TableIdentifier("src2")),"s2");
        assertEqual(
                "SELECT * FROM (src1 s1 INNER JOIN src2 s2 ON s1.id = s2.id) dst(a, b, c, d)",
                select(new UnresolvedSubqueryColumnAliases(
                        Arrays.asList("a", "b", "c", "d"),
                        new SubqueryAlias(
                                "dst",
                                join(src1,src2, new Inner(), new EqualTo(new UnresolvedAttribute("s1.id"), new UnresolvedAttribute("s2.id"))))
                ),star()));
    }



    @Test
    public void testInlineTable() {
        assertEqual("values 1, 2, 3, 4",
                new UnresolvedInlineTable(
                        Arrays.asList("col1"),
                        Arrays.asList(
                                Arrays.asList(Literal.build(new Integer(1))),
                                Arrays.asList(Literal.build(new Integer(2))),
                                Arrays.asList(Literal.build(new Integer(3))),
                                Arrays.asList(Literal.build(new Integer(4)))
                        )));

        assertEqual(
                "values (1, 'a'), (2, 'b') as tbl(a, b)",
                new SubqueryAlias("tbl",
                        new UnresolvedInlineTable(
                                Arrays.asList("a", "b"),
                                Arrays.asList(
                                        Arrays.asList(Literal.build(new Integer(1)), Literal.build(new String("a"))),
                                        Arrays.asList(Literal.build(new Integer(2)), Literal.build(new String("b")))
                                ))));
    }


    @Test
    public void testUnEqual() {//("simple select query with !> and !<") {
        // !< is equivalent to >=
        assertEqual("select a, b from db.c where x !< 1",
                select(
                        where(table("db", "c"), new GreaterThanOrEqual(new UnresolvedAttribute("x"), Literal.build(new Integer(1)))),
                        new UnresolvedAttribute("a"), new UnresolvedAttribute("b")));
        // !> is equivalent to <=
        assertEqual("select a, b from db.c where x !> 1",
                select(where(table("db", "c"), new LessThanOrEqual(new UnresolvedAttribute("x"), Literal.build(new Integer(1)))),
                        new UnresolvedAttribute("a"), new UnresolvedAttribute("b")));
    }


    @Test
    public void testHint() {
        intercept("SELECT /*+ HINT() */ * FROM t","mismatched input");

        intercept("SELECT /*+ INDEX(a b c) */ * from default.t","mismatched input 'b' expecting");

        intercept("SELECT /*+ INDEX(a b c) */ * from default.t","mismatched input 'b' expecting");

        assertEqual(
                "SELECT /*+ BROADCASTJOIN(u) */ * FROM t",
                new UnresolvedHint("BROADCASTJOIN", Arrays.asList(new UnresolvedAttribute("u")), select(table("t"),star()))
        );


        assertEqual(
                "SELECT /*+ MAPJOIN(u) */ * FROM t",
                new UnresolvedHint("MAPJOIN", Arrays.asList(new UnresolvedAttribute("u")), select(table("t"),star()))
        );


        assertEqual(
                "SELECT /*+ STREAMTABLE(a,b,c) */ * FROM t",
                new UnresolvedHint("STREAMTABLE", Arrays.asList(
                        new UnresolvedAttribute("a"),
                        new UnresolvedAttribute("b"),
                        new UnresolvedAttribute("c")
                        ), select(table("t"),star()))
        );


        assertEqual(
                "SELECT /*+ INDEX(t, emp_job_ix) */ * FROM t",
                new UnresolvedHint("INDEX", Arrays.asList(
                        new UnresolvedAttribute("t"),
                        new UnresolvedAttribute("emp_job_ix")
                ), select(table("t"),star()))
        );

        assertEqual(
                "SELECT /*+ MAPJOIN(`default.t`) */ * from `default.t`",
                new UnresolvedHint("MAPJOIN", Arrays.asList(
                        UnresolvedAttribute.quoted("default.t")
                ), select(table("default.t"),star()))
        );

        assertEqual(
                "SELECT /*+ MAPJOIN(t) */ a from t where true group by a order by a",
                orderBy(
                        new UnresolvedHint("MAPJOIN", Arrays.asList(
                            new UnresolvedAttribute("t")
                        ),
                        groupBy(
                                where(table("t"), Literal.build(new Boolean(true))),
                                Arrays.asList(new UnresolvedAttribute("a")),
                                Arrays.asList(new UnresolvedAttribute("a")))),
                        new SortOrder(new UnresolvedAttribute("a"), new Ascending())));

        assertEqual(
                "SELECT /*+ COALESCE(10) */ * FROM t",
                new UnresolvedHint("COALESCE", Arrays.asList(
                        Literal.build(new Integer(10))
                ), select(table("t"),star()))
        );


        assertEqual(
                "SELECT /*+ REPARTITION(100) */ * FROM t",
                new UnresolvedHint("REPARTITION", Arrays.asList(
                        Literal.build(new Integer(100))
                ), select(table("t"),star()))
        );



        assertEqual(
                "INSERT INTO s SELECT /*+ REPARTITION(100), COALESCE(500), COALESCE(10) */ * FROM t",
                new InsertIntoTable( table("s"), new HashMap<String,String>(),
                        new UnresolvedHint("REPARTITION", Arrays.asList(
                        Literal.build(new Integer(100))),
                                new UnresolvedHint("COALESCE", Arrays.asList(
                                        Literal.build(new Integer(500))),
                                        new UnresolvedHint("COALESCE", Arrays.asList(
                                                Literal.build(new Integer(10))),
                                                select(table("t"),star())
                                                )
                                )),false,false));



        assertEqual(
                "SELECT /*+ BROADCASTJOIN(u), REPARTITION(100) */ * FROM t",
                        new UnresolvedHint("BROADCASTJOIN", Arrays.asList(
                                new UnresolvedAttribute("u")),
                                new UnresolvedHint("REPARTITION", Arrays.asList(
                                        Literal.build(new Integer(100))),
                                                select(table("t"),star())

                                )));

        intercept("SELECT /*+ COALESCE(30 + 50) */ * FROM t", "mismatched input");
    }


    @Test
    public void testHintWithExpr() {//("SPARK-20854: select hint syntax with expressions")
        assertEqual(
                "SELECT /*+ HINT1(a, array(1, 2, 3)) */ * from t",
                new UnresolvedHint("HINT1", Arrays.asList(
                        new UnresolvedAttribute("a"),
                        new UnresolvedFunction("array", Arrays.asList(
                                Literal.build(new Integer(1)),
                                Literal.build(new Integer(2)),
                                Literal.build(new Integer(3))
                        ), false)
                ), select(table("t"), star()))
        );


        assertEqual(
                "SELECT /*+ HINT1(a, 5, 'a', b) */ * from t",
                new UnresolvedHint("HINT1", Arrays.asList(
                        new UnresolvedAttribute("a"),
                        Literal.build(new Integer(5)),
                        Literal.build(new String("a")),
                        new UnresolvedAttribute("b")
                ), select(table("t"), star()))
        );


        assertEqual(
                "SELECT /*+ HINT1('a', (b, c), (1, 2)) */ * from t",
                new UnresolvedHint("HINT1", Arrays.asList(
                        Literal.build(new String("a")),
                        CreateStruct.build(Arrays.asList(new UnresolvedAttribute("b"), new UnresolvedAttribute("c"))),
                        CreateStruct.build(Arrays.asList(Literal.build(new Integer(1)), Literal.build(new Integer(2))))
                ), select(table("t"), star()))
        );
    }

    @Test
    public void testMultipleHints() {//("SPARK-20854: multiple hints")
        assertEqual(
                "SELECT /*+ HINT1(a, 1) hint2(b, 2) */ * from t",
                new UnresolvedHint("HINT1",
                        Arrays.asList(
                                new UnresolvedAttribute("a"),
                                Literal.build(new Integer(1))
                        ),
                        new UnresolvedHint("hint2",
                                Arrays.asList(
                                        new UnresolvedAttribute("b"),
                                        Literal.build(new Integer(2))
                                ),
                                select(table("t"),star())
                        )
                )
                );


        assertEqual(
                "SELECT /*+ HINT1(a, 1) */ /*+ hint2(b, 2) */ * from t",
                new UnresolvedHint("HINT1",
                        Arrays.asList(
                                new UnresolvedAttribute("a"),
                                Literal.build(new Integer(1))
                        ),
                        new UnresolvedHint("hint2",
                                Arrays.asList(
                                        new UnresolvedAttribute("b"),
                                        Literal.build(new Integer(2))
                                ),
                                select(table("t"),star())
                        )
                )
        );



        assertEqual(
                "SELECT /*+ HINT1(a, 1), hint2(b, 2) */ /*+ hint3(c, 3) */ * from t",
                new UnresolvedHint("HINT1",
                        Arrays.asList(
                                new UnresolvedAttribute("a"),
                                Literal.build(new Integer(1))
                        ),
                        new UnresolvedHint("hint2",
                                Arrays.asList(
                                        new UnresolvedAttribute("b"),
                                        Literal.build(new Integer(2))
                                ),
                                new UnresolvedHint("hint3",
                                        Arrays.asList(
                                                new UnresolvedAttribute("c"),
                                                Literal.build(new Integer(3))
                                        ),
                                select(table("t"),star())
                                )
                        )
                )
        );
    }


    @Test
    public void TestTrim(){
        intercept("select ltrim(both 'S' from 'SS abc S'", "missing ')' at '<EOF>'");
        intercept("select rtrim(trailing 'S' from 'SS abc S'", "missing ')' at '<EOF>'");

        assertEqual(
                "SELECT TRIM(BOTH '@$%&( )abc' FROM '@ $ % & ()abc ' )",
                select(new OneRowRelation(), new UnresolvedFunction("TRIM",
                        Arrays.asList(
                                Literal.build(new String("@$%&( )abc")),
                                Literal.build(new String("@ $ % & ()abc "))
                        ),false
                )));

                assertEqual(
                        "SELECT TRIM(LEADING 'c []' FROM '[ ccccbcc ')",
                        select(new OneRowRelation(), new UnresolvedFunction("ltrim",
                                Arrays.asList(
                                        Literal.build(new String("c []")),
                                        Literal.build(new String("[ ccccbcc "))
                                ),false
                        )));

                        assertEqual(
                                "SELECT TRIM(TRAILING 'c&^,.' FROM 'bc...,,,&&&ccc')",
                                select(new OneRowRelation(), new UnresolvedFunction("rtrim",
                                        Arrays.asList(
                                                Literal.build(new String("c&^,.")),
                                                Literal.build(new String("bc...,,,&&&ccc"))
                                        ) ,false
                                )));
    }


    @Test
    public void testPrecedenceOfSetOperations(){
        LogicalPlan a = select(table("a"),star());
        LogicalPlan b = select(table("b"),star());
        LogicalPlan c = select(table("c"),star());
        LogicalPlan d = select(table("d"),star());


        String query1 =" SELECT * FROM a UNION SELECT * FROM b EXCEPT SELECT * FROM c INTERSECT SELECT * FROM d";
        String query2 ="SELECT * FROM a UNION SELECT * FROM b EXCEPT ALL SELECT * FROM c INTERSECT ALL SELECT * FROM d";
        assertEqual(query1,
                except(
                        new Distinct(union(a,b)),
                        intersect(c,d,false),false));

        assertEqual(query2,
                except(new Distinct(union(a,b)),
                        intersect(c,d, true), true));

        //skip
//        // Now disable precedence enforcement to verify the old behaviour.
//        withSQLConf(SQLConf.LEGACY_SETOPS_PRECEDENCE_ENABLED.key -> "true") {
//            assertEqual(query1,
//                    Distinct(a.union(b)).except(c, isAll = false).intersect(d, isAll = false))
//            assertEqual(query2, Distinct(a.union(b)).except(c, isAll = true).intersect(d, isAll = true))
//        }
//
//        // Explicitly enable the precedence enforcement
//        withSQLConf(SQLConf.LEGACY_SETOPS_PRECEDENCE_ENABLED.key -> "false") {
//            assertEqual(query1,
//                    Distinct(a.union(b)).except(c.intersect(d, isAll = false), isAll = false))
//            assertEqual(query2, Distinct(a.union(b)).except(c.intersect(d, isAll = true), isAll = true))
//        }


    }

}
