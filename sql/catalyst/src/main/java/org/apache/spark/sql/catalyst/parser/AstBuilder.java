package org.apache.spark.sql.catalyst.parser;

import com.sun.org.apache.xpath.internal.functions.Function2Args;
import javafx.util.Pair;
import lombok.Data;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.RuleNode;
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute;
import org.apache.spark.sql.catalyst.analysis.unresolved.UnresolvedAlias;
import org.apache.spark.sql.catalyst.expressions.*;
import org.apache.spark.sql.catalyst.expressions.grouping.Cube;
import org.apache.spark.sql.catalyst.expressions.grouping.Rollup;
import org.apache.spark.sql.catalyst.expressions.namedExpressions.Alias;
import org.apache.spark.sql.catalyst.expressions.namedExpressions.NamedExpression;
import org.apache.spark.sql.catalyst.expressions.predicates.Predicate;
import org.apache.spark.sql.catalyst.expressions.windowExpressions.WindowSpec;
import org.apache.spark.sql.catalyst.expressions.windowExpressions.WindowSpecDefinition;
import org.apache.spark.sql.catalyst.expressions.windowExpressions.WindowSpecReference;
import org.apache.spark.sql.catalyst.identifiers.FunctionIdentifier;
import org.apache.spark.sql.catalyst.identifiers.TableIdentifier;
import org.apache.spark.sql.catalyst.plans.joinTypes.*;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.ScriptInputOutputSchema;
import org.apache.spark.sql.catalyst.plans.logical.ScriptTransformation;
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias;
import org.apache.spark.sql.catalyst.plans.logical.basicLogicalOperators.*;
import org.apache.spark.sql.internal.SQLConf;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.spark.sql.catalyst.parser.ParserUtils;
import org.apache.spark.sql.types.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.spark.sql.catalyst.expressions.complexTypeCreator.CreateStruct;
/**
 * Created by kenya on 2019/1/18.
 */
public class AstBuilder extends SqlBaseBaseVisitor<Object> {
    SQLConf conf;

    public AstBuilder(){
        this.conf = new SQLConf();
    }

    public AstBuilder(SQLConf conf){
        this.conf = conf;
    }

    protected <T> T typedVisit(ParseTree ctx){
        return (T)ctx.accept(this);
    }

    @Override
    public Object visitChildren(RuleNode node){
        if (node.getChildCount() == 1) {
            return node.getChild(0).accept(this);
        } else {
            return null;
        }
    }

    @Override
    public LogicalPlan visitSingleStatement(SqlBaseParser.SingleStatementContext ctx) {
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.SingleStatementContext, LogicalPlan>() {
            @Override
            public LogicalPlan apply(SqlBaseParser.SingleStatementContext ctx) {
                return (LogicalPlan) visit(ctx.statement());
            }
        });
    }



    @Override
    public Expression visitSingleExpression(SqlBaseParser.SingleExpressionContext ctx){
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.SingleExpressionContext, Expression>() {
                    @Override
                    public Expression apply(SqlBaseParser.SingleExpressionContext singleExpressionContext) {
                        return (Expression)visitNamedExpression(ctx.namedExpression());
                    }
                });
    }

    @Override
    public TableIdentifier visitSingleTableIdentifier(SqlBaseParser.SingleTableIdentifierContext ctx){
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.SingleTableIdentifierContext, TableIdentifier>() {
            @Override
            public TableIdentifier apply(SqlBaseParser.SingleTableIdentifierContext singleTableIdentifierContext) {
                return (TableIdentifier)visitTableIdentifier(ctx.tableIdentifier());
            }
        });
    }

    @Override
    public FunctionIdentifier visitSingleFunctionIdentifier(SqlBaseParser.SingleFunctionIdentifierContext ctx){
            return  ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.SingleFunctionIdentifierContext, FunctionIdentifier>() {
                        @Override
                        public FunctionIdentifier apply(SqlBaseParser.SingleFunctionIdentifierContext singleFunctionIdentifierContext) {
                            return (FunctionIdentifier)visitFunctionIdentifier(ctx.functionIdentifier());
                        }
                    });
    }

    @Override
    public DataType visitSingleDataType(SqlBaseParser.SingleDataTypeContext ctx){
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.SingleDataTypeContext, DataType>() {
            @Override
            public DataType apply(SqlBaseParser.SingleDataTypeContext singleDataTypeContext) {
                return (DataType)visitSparkDataType(ctx.dataType());
            }
        });
    }

    @Override
    public StructType visitSingleTableSchema(SqlBaseParser.SingleTableSchemaContext ctx){
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.SingleTableSchemaContext, StructType>() {
            @Override
            public StructType apply(SqlBaseParser.SingleTableSchemaContext singleTableSchemaContext) {
                return new StructType(visitColTypeList(ctx.colTypeList()));
            }
        });
    }

    protected LogicalPlan plan(ParserRuleContext tree){
        return typedVisit(tree);
    }

    @Override
    public LogicalPlan visitQuery(SqlBaseParser.QueryContext ctx){
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.QueryContext, LogicalPlan>() {
            @Override
            public LogicalPlan apply(SqlBaseParser.QueryContext queryContext) {
                LogicalPlan query = plan(ctx.queryNoWith());

                return optional(query, ctx, new Function<SqlBaseParser.QueryContext, LogicalPlan>() {
                            @Override
                            public LogicalPlan apply(SqlBaseParser.QueryContext queryContext) {

                                List<Pair<String, SubqueryAlias>> ctes = new ArrayList<>();
                                for(SqlBaseParser.NamedQueryContext nCtx : queryContext.ctes().namedQuery()){
                                    SubqueryAlias namedQuery = visitNamedQuery(nCtx);
                                    ctes.add(new Pair<>(namedQuery.alias(), namedQuery));
                                }
                                ParserUtils.checkDuplicateKeys(ctes, queryContext);
                                return new With(query, ctes);
                            }
                        }
                );
            }
        });
    }


    @Override
    public SubqueryAlias visitNamedQuery(SqlBaseParser.NamedQueryContext ctx){
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.NamedQueryContext, SubqueryAlias>() {
                    @Override
                    public SubqueryAlias apply(SqlBaseParser.NamedQueryContext namedQueryContext) {
                        return new SubqueryAlias(namedQueryContext.name.getText(), plan(namedQueryContext.query()));
                    }
                });
    }



    @Override
    public LogicalPlan visitMultiInsertQuery(SqlBaseParser.MultiInsertQueryContext ctx){

        LogicalPlan from = visitFromClause(ctx.fromClause());

        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.MultiInsertQueryContext, LogicalPlan>() {
                    @Override
                    public LogicalPlan apply(SqlBaseParser.MultiInsertQueryContext context) {

                        List<LogicalPlan> inserts = new ArrayList<>();
                        for(SqlBaseParser.MultiInsertQueryBodyContext body: context.multiInsertQueryBody()){
                            ParserUtils.validate(
                                    ()->{return body.querySpecification().fromClause() == null;},
                                    "Multi-Insert queries cannot have a FROM clause in their individual SELECT statements",
                                    body);

                            withQuerySpecification();



                        }

                        return null;
                    }
                }
        );
    }


    protected ScriptInputOutputSchema withScriptIOSchema(
            SqlBaseParser.QuerySpecificationContext ctx,
            SqlBaseParser.RowFormatContext inRowFormat,
            Token recordWriter,
            SqlBaseParser.RowFormatContext outRowFormat,
            Token recordReader,
            boolean schemaLess){
        throw new ParseException("Script Transform is not supported", ctx);
    }

    @Override
    public LogicalPlan visitFromClause(SqlBaseParser.FromClauseContext ctx){
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.FromClauseContext, LogicalPlan>() {
            @Override
            public LogicalPlan apply(SqlBaseParser.FromClauseContext fromClauseContext) {
            LogicalPlan from = ParserUtils.foldLeft(ctx.relation(), null, (left,relation) -> {
                LogicalPlan right = plan(relation.relationPrimary());
                LogicalPlan join = null;
                if (left == null) {
                    join = right;
                } else {
                    join = new Join(left, right, new Inner(), null);
                }
                return withJoinRelations(join, relation);
            });

            if(fromClauseContext.pivotClause()!=null){
                if(!fromClauseContext.lateralView().isEmpty()){
                    throw new ParseException("LATERAL cannot be used together with PIVOT in FROM clause", ctx);
                }
                return withPivot(fromClauseContext.pivotClause(),from);
            }

//                return withJoinRelations(join, relation);
//            }
//            }
//            );
//
//        });
//
//        if (ctx.pivotClause() != null) {
//            if (!ctx.lateralView.isEmpty) {
//                throw new ParseException("LATERAL cannot be used together with PIVOT in FROM clause", ctx)
//            }
//            withPivot(ctx.pivotClause, from)
//        } else {
//            ctx.lateralView.asScala.foldLeft(from)(withGenerate)
//        }
                return null;
            }
        });
    }


    private LogicalPlan withPivot(
            SqlBaseParser.PivotClauseContext ctx,
            LogicalPlan query){
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.PivotClauseContext, LogicalPlan>() {
            @Override
            public LogicalPlan apply(SqlBaseParser.PivotClauseContext pivotClauseContext){
                List<Expression> aggregates = new ArrayList<>();
                SqlBaseParser.NamedExpressionSeqContext namedSeq = pivotClauseContext.aggregates;
                if(namedSeq!=null) {
                    for (SqlBaseParser.NamedExpressionContext named : namedSeq.namedExpression()) {
                        aggregates.add(typedVisit(named));
                    }
                }

                Expression pivotColumn;
                if(pivotClauseContext.pivotColumn().identifiers.size() ==1){
                    pivotColumn = UnresolvedAttribute.quoted(pivotClauseContext.pivotColumn().identifier.getText());
                }else{
                    List<Expression> expressions = new ArrayList<>();
                    for(SqlBaseParser.IdentifierContext identifier: pivotClauseContext.pivotColumn().identifiers){
                        Expression expr = UnresolvedAttribute.quoted(identifier.getText());
                        expressions.add(expr);
                    }
                    CreateStruct createStruct = new CreateStruct();
                    pivotColumn = createStruct.apply(expressions);
                }

                List<Expression> pivotValues = new ArrayList<>();
                for(SqlBaseParser.PivotValueContext pivotValueContext: pivotClauseContext.pivotValues){
                    pivotValues.add(visitPivotValue(pivotValueContext));
                }
                return new Pivot(null, pivotColumn, pivotValues, aggregates, query);
            }
        });

    }

    @Override
    public Expression visitPivotValue(SqlBaseParser.PivotValueContext ctx){
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.PivotValueContext, Expression>() {

            @Override
            public Expression apply(SqlBaseParser.PivotValueContext pivotValueContext) {
                Expression e = expression(pivotValueContext.expression());
                if(pivotValueContext.identifier()!=null){
                    return new Alias(e,pivotValueContext.getText());
                }else{
                    return e;
                }
            }
        });
    }




    private LogicalPlan withJoinRelations(LogicalPlan base, SqlBaseParser.RelationContext ctx){
        return ParserUtils.foldLeft(ctx.joinRelation(), base, (left,join )->{

            return ParserUtils.withOrigin(join, new Function<SqlBaseParser.JoinRelationContext, LogicalPlan>() {
                @Override
                public LogicalPlan apply(SqlBaseParser.JoinRelationContext joinRelationContext){
                    JoinType baseJoinType;
                    SqlBaseParser.JoinTypeContext jt = joinRelationContext.joinType();
                    baseJoinType = new Inner();
                    if(jt!=null){
                        if(jt.CROSS()!=null){
                            baseJoinType = new Cross();
                        }else if(jt.FULL()!=null){
                            baseJoinType = new FullOuter();
                        }else if(jt.SEMI()!=null){
                            baseJoinType = new LeftSemi();
                        }else if(jt.ANTI()!=null){
                            baseJoinType = new LeftAnti();
                        }else if(jt.LEFT()!=null){
                            baseJoinType = new LeftOuter();
                        }else if(jt.RIGHT()!=null){
                            baseJoinType = new RightOuter();
                        }
                    }
                    Expression condition = null;
                    JoinType joinType = null;

                    SqlBaseParser.JoinCriteriaContext criteria = joinRelationContext.joinCriteria();
                    if(criteria!=null){
                        if(criteria.USING()!=null){
                            joinType = new UsingJoin(baseJoinType, visitIdentifierList(criteria.identifierList());
                        }else if(criteria.booleanExpression()!=null){
                            condition = expression(criteria.booleanExpression());
                        }
                    }else{
                        if(joinRelationContext.NATURAL()!=null){
                            if(baseJoinType.getClass()==Cross.class){
                                throw new ParseException("NATURAL CROSS JOIN is not supported", ctx);
                            }
                            joinType = new NaturalJoin(baseJoinType);
                        }else{
                            joinType = baseJoinType;
                        }
                    }
                    return new Join(left, plan(join.right), joinType, condition);
                }
            });

        });
    }

    @Override
    public List<String> visitIdentifierList(SqlBaseParser.IdentifierListContext ctx) {
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.IdentifierListContext, List<String>>() {
            @Override
            public List<String> apply(SqlBaseParser.IdentifierListContext identifierListContext) {
                return visitIdentifierSeq(identifierListContext.identifierSeq());
            }
        });

    }

    @Override
    public List<String> visitIdentifierSeq(SqlBaseParser.IdentifierSeqContext ctx){
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.IdentifierSeqContext , List<String>>() {
            @Override
            public List<String> apply(SqlBaseParser.IdentifierSeqContext identifierSeqContext){
                List<String> result = new ArrayList<>();
                for(SqlBaseParser.IdentifierContext identifierContext: ctx.identifier()){
                    result.add(identifierContext.getText());
                }
                return result;
            }
        });
    }

    /**
     * Add a query specification to a logical plan. The query specification is the core of the logical
     * plan, this is where sourcing (FROM clause), transforming (SELECT TRANSFORM/MAP/REDUCE),
     * projection (SELECT), aggregation (GROUP BY ... HAVING ...) and filtering (WHERE) takes place.
     *
     * Note that query hints are ignored (both by the parser and the builder).
     */


    interface FilterOperation {
        LogicalPlan apply(SqlBaseParser.BooleanExpressionContext ctx , LogicalPlan plan);
    }

    interface WithHavingOperation {
        LogicalPlan apply(SqlBaseParser.BooleanExpressionContext ctx , LogicalPlan plan);
    }

    private LogicalPlan withQuerySpecification(
            SqlBaseParser.QuerySpecificationContext ctx,LogicalPlan relation){

        FilterOperation filter = (SqlBaseParser.BooleanExpressionContext c, LogicalPlan p) -> new Filter(expression(c),p);

        WithHavingOperation withHaving = (SqlBaseParser.BooleanExpressionContext c, LogicalPlan p) ->{
            Expression predicate = expression(c);
            if(predicate instanceof Predicate){
                return new Filter(predicate,p);
            }else{
                return new Filter(new Cast(predicate, new BooleanType()),p);
            }
        };


        return  ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.QuerySpecificationContext, LogicalPlan>() {
            @Override
            public LogicalPlan apply(SqlBaseParser.QuerySpecificationContext querySpecificationContext) {


                List<Expression> expressions = new ArrayList<>();
                for(SqlBaseParser.NamedExpressionContext nec : querySpecificationContext.namedExpressionSeq().namedExpression()){
                    expressions.add((Expression)typedVisit(nec));
                }

                int specType = SqlBaseParser.SELECT;
                if(querySpecificationContext.kind!=null){
                    specType = querySpecificationContext.kind.getType();
                }
                switch (specType){
                    case SqlBaseParser.MAP:
                    case SqlBaseParser.REDUCE :
                    case SqlBaseParser.TRANSFORM: {
                        LogicalPlan withFilter = filter.apply(querySpecificationContext.where, relation);

                        //List<AttributeReference> attributes;
                        List<AttributeReference> attributes;
                        boolean schemaLess;

                        if (querySpecificationContext.colTypeList() != null) {
                            attributes = createSchema(querySpecificationContext.colTypeList()).toAttributes();
                            schemaLess = false;
                        } else if (querySpecificationContext.identifierSeq() != null) {
                            attributes = new ArrayList<>();
                            List<String> identifierSeql = visitIdentifierSeq(querySpecificationContext.identifierSeq());
                            for (String name : visitIdentifierSeq(querySpecificationContext.identifierSeq())) {
                                attributes.add(new AttributeReference(name, new StringType(), true));
                            }
                            schemaLess = false;
                        } else {
                            schemaLess = true;
                            attributes = new ArrayList<>();
                            attributes.add(new AttributeReference("key", new StringType()));
                            attributes.add(new AttributeReference("value", new StringType()));
                        }
                        return new ScriptTransformation(
                                expressions,
                                ParserUtils.string(querySpecificationContext.script),
                                attributes,
                                withFilter,
                                withScriptIOSchema(
                                        ctx, querySpecificationContext.inRowFormat, querySpecificationContext.recordWriter, querySpecificationContext.outRowFormat, querySpecificationContext.recordReader, schemaLess));
                    }
                    case SqlBaseParser.SELECT: {

                        LogicalPlan withLateralView = ParserUtils.foldLeft(querySpecificationContext.lateralView(), relation, (query, left) -> {
                            return withGenerate(query, left);
                        });

                        LogicalPlan withFilter = ParserUtils.optionalMap(withLateralView,querySpecificationContext.where, (c,p) -> {
                            return filter.apply(c,p);
                        });

                        List<NamedExpression>namedExpressions = new ArrayList<>();
                        for(Expression expression: expressions){
                            if(expression instanceof NamedExpression){
                                namedExpressions.add((NamedExpression) expression);
                            }else{
                                namedExpressions.add( new UnresolvedAlias(expression));
                            }
                        }

                        Function<List<NamedExpression>, LogicalPlan>createProject = new Function<List<NamedExpression>, LogicalPlan>() {
                            @Override
                            public LogicalPlan apply(List<NamedExpression> namedExpressions) {
                                if(namedExpressions.size()>0){
                                    return new Project(namedExpressions,withFilter);
                                }else{
                                    return withFilter;
                                }
                            }
                        };

                        Function withProject = new Function<SqlBaseParser.QuerySpecificationContext, LogicalPlan>(){
                            @Override
                            public LogicalPlan apply(SqlBaseParser.QuerySpecificationContext qctx){
                                if(qctx.aggregation()==null && qctx.having !=null){
                                    if(conf.getConf(SQLConf.LEGACY_HAVING_WITHOUT_GROUP_BY_AS_WHERE)){
                                        return withHaving.apply(qctx.having, createProject.apply(namedExpressions));
                                    }else{
                                        return withHaving.apply(qctx.having, new Aggregate(null, namedExpressions, withFilter));
                                    }
                                }else if (qctx.aggregation() != null) {
                                    LogicalPlan aggregate = withAggregation(qctx.aggregation(), namedExpressions, withFilter);
                                    return ParserUtils.optionalMap(aggregate,qctx.having,(c,p) -> {
                                        return withHaving.apply(c,p);
                                    } );
                                }else{
                                    return createProject.apply(namedExpressions);
                                }
                            }

                        };

                        Function withDistinct = new Function<SqlBaseParser.QuerySpecificationContext, LogicalPlan>() {
                            @Override
                            public LogicalPlan apply(SqlBaseParser.QuerySpecificationContext o) {
                                if(o.setQuantifier()!=null && o.setQuantifier().DISTINCT()!=null){
                                    return new Distinct((LogicalPlan) withProject.apply(o));
                                }else{
                                    return (LogicalPlan) withProject.apply(o);
                                }
                            }
                        };


                        ParserUtils.optionalMap(withFilter,querySpecificationContext.windows(),withWin)
                        val withWindow = withDistinct.optionalMap(windows)(withWindows)




                    }

                      //  val withLateralView = ctx.lateralView.asScala.foldLeft(relation)(withGenerate)
                        ;
                }





                //withQuerySpecificationInnerInterface filter = (ctx1, plan1) -> return new Filter(ctx1, plan1);




                return null;
            }
        });

    }




    /**
     * Add a [[WithWindowDefinition]] operator to a logical plan.
     */
    private LogicalPlan withWindows(
            SqlBaseParser.WindowsContext ctx,
            LogicalPlan query){
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.WindowsContext, LogicalPlan>() {
            @Override
            public LogicalPlan apply(SqlBaseParser.WindowsContext wctx){

                Map<String, WindowSpec> baseWindowMap = new HashMap<>();
                for(SqlBaseParser.NamedWindowContext namedWindowContext: wctx.namedWindow()){
                    baseWindowMap.put(namedWindowContext.identifier().getText(),typedVisit(namedWindowContext.windowSpec()));
                }

                List<WindowSpec> windowMapView = new ArrayList<>();
                for(WindowSpec windowSpec: baseWindowMap.values()){
                    if(windowSpec instanceof WindowSpecReference){
                        String name = ((WindowSpecReference) windowSpec).getName();
                        WindowSpec spec = baseWindowMap.get(name);
                        if(spec instanceof WindowSpecDefinition){
                            windowMapView.add(spec);
                        }else if(spec==null){
                            throw new ParseException("Cannot resolve window reference '$name'", ctx);
                        }else{
                            throw new ParseException("Window reference '$name' is not a window specification", ctx);
                        }
                    }else if(windowSpec instanceof WindowSpecDefinition){
                        windowMapView.add(windowSpec);
                    }
                }

                Map<String ,WindowSpecDefinition>windowDefinitions = new HashMap<>();
                for(){

                }

                return new WithWindowDefinition(windowMapView, query);



            }
                return null;
            }
        });

        {
        // Collect all window specifications defined in the WINDOW clause.
        val baseWindowMap = ctx.namedWindow.asScala.map {
            wCtx =>
            (wCtx.identifier.getText, typedVisit[WindowSpec](wCtx.windowSpec))
        }.toMap

        // Handle cases like
        // window w1 as (partition by p_mfgr order by p_name
        //               range between 2 preceding and 2 following),
        //        w2 as w1
        val windowMapView = baseWindowMap.mapValues {
            case WindowSpecReference(name) =>
                baseWindowMap.get(name) match {
                case Some(spec: WindowSpecDefinition) =>
                    spec
                case Some(ref) =>
                    throw new ParseException(s"Window reference '$name' is not a window specification", ctx)
                case None =>
                    throw new ParseException(s"Cannot resolve window reference '$name'", ctx)
            }
            case spec: WindowSpecDefinition => spec
        }

        // Note that mapValues creates a view instead of materialized map. We force materialization by
        // mapping over identity.
        WithWindowDefinition(windowMapView.map(identity), query)
    }


    private LogicalPlan withAggregation(
            SqlBaseParser.AggregationContext ctx,
            List<NamedExpression>selectExpressions,
            LogicalPlan query){

                return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.AggregationContext, LogicalPlan>() {
                    @Override
                    public LogicalPlan apply(SqlBaseParser.AggregationContext aggregationContext){
                        List<Expression> groupByExpressions = expressionList(aggregationContext.groupingExpressions);
                        if(aggregationContext.GROUPING()!=null){
                            List<List<Expression>> selectedGroupByExprs = new ArrayList<>();
                            for(SqlBaseParser.GroupingSetContext groupingSetContext:aggregationContext.groupingSet()){
                                List<Expression> subList = new ArrayList<>();
                                for(SqlBaseParser.ExpressionContext expressionContext:groupingSetContext.expression()){
                                    subList.add(expression(expressionContext));
                                }
                                selectedGroupByExprs.add(subList);
                            }
                            return new GroupingSets(selectedGroupByExprs, groupByExpressions, query, selectExpressions);
                        }else{
                            List<Expression>mappedGroupByExpressions = new ArrayList<>();
                            if(aggregationContext.CUBE()!=null){
                                mappedGroupByExpressions.add( new Cube(groupByExpressions));
                            } else if (aggregationContext.ROLLUP() != null) {
                                mappedGroupByExpressions.add( new Rollup(groupByExpressions));
                            } else {
                                mappedGroupByExpressions = groupByExpressions;
                            }

                            return new Aggregate(mappedGroupByExpressions, selectExpressions, query);
                        }
                    }
                });
    }

    protected Expression expression(ParserRuleContext ctx){
                return typedVisit(ctx);
    }


    private List<Expression> expressionList(List<SqlBaseParser.ExpressionContext> trees){

        List<Expression> expressions = new ArrayList<>();
        for(SqlBaseParser.ExpressionContext expressionContext : trees){
            expressions.add(expression(expressionContext));
        }
        return expressions;
    }



    protected StructType createSchema(SqlBaseParser.ColTypeListContext ctx){
        return new StructType(visitColTypeList(ctx));
    }


//    @Override
//    public List<StructField> visitColTypeList(SqlBaseParser.ColTypeListContext ctx){
//        List<StructField> structFields = new ArrayList<>();
//        for(SqlBaseParser.ColTypeContext colTypeContext :ctx.colType()){
//
//        }
//        return structFields;
//    }





    private LogicalPlan withGenerate(
            LogicalPlan query,
            SqlBaseParser.LateralViewContext ctx){
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.LateralViewContext, LogicalPlan>() {
            @Override
            public LogicalPlan apply(SqlBaseParser.LateralViewContext lateralViewContext){
                List<Expression> expressions = expressionList(lateralViewContext.expression());

                List<Attribute> attributes = new ArrayList<>();
                for(SqlBaseParser.IdentifierContext identifierContext: lateralViewContext.colName){
                    attributes.add(UnresolvedAttribute.apply(identifierContext.getText()));
                }

                return new Generate(
                        new UnresolvedGenerator(visitFunctionName(lateralViewContext.qualifiedName()), expressions),
                        null,
                        lateralViewContext.OUTER() != null,
                        lateralViewContext.tblName.getText().toLowerCase(),
                        attributes,
                        query
                );


                return null;
            }
        });
        val
        Generate(
                UnresolvedGenerator(visitFunctionName(ctx.qualifiedName), expressions),
                unrequiredChildIndex = Nil,
                outer = ctx.OUTER != null,
                // scalastyle:off caselocale
                Some(ctx.tblName.getText.toLowerCase),
                // scalastyle:on caselocale
                ctx.colName.asScala.map(_.getText).map(UnresolvedAttribute.apply),
                query)
    }























    public <T>LogicalPlan optional(LogicalPlan plan, T ctx, Function<T, LogicalPlan> f){
        if(ctx!=null){
            return f.apply(ctx);
        }else{
            return plan;
        }
    }

    private DataType visitSparkDataType(SqlBaseParser.DataTypeContext ctx){
         return HiveStringType.replaceCharType((DataType)typedVisit(ctx));
    }

    @Override
    public List<StructField> visitColTypeList(SqlBaseParser.ColTypeListContext ctx){
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.ColTypeListContext, List<StructField>>() {
                    @Override
                    public List<StructField> apply(SqlBaseParser.ColTypeListContext colTypeListContext) {
                        //ctx.colType().asScala.map(visitColType);
                        List<SqlBaseParser.ColTypeContext>colTypes = ctx.colType();

                        for(SqlBaseParser.ColTypeContext colType: colTypes){

                        }

                        return null;
                    }
                });
    }

    @Override
    public StructField  visitColType(SqlBaseParser.ColTypeContext ctx){
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.ColTypeContext, StructField>() {
            @Override
            public StructField apply(SqlBaseParser.ColTypeContext colTypeContext) {

                MetadataBuilder builder = new MetadataBuilder();
                if(ctx.STRING()!=null){
                    builder.putString("comment", ParserUtils.string(ctx.STRING()));
                }

                DataType rawDataType = typedVisit(ctx.dataType());
                DataType cleanedDataType = HiveStringType.replaceCharType(rawDataType);
                if (rawDataType != cleanedDataType) {
                    builder.putString(types.HIVE_TYPE_STRING, rawDataType.catalogString());
                }
                return new StructField(ctx.identifier().getText(), cleanedDataType, true, builder.build());
            }
        });
    }

    protected FunctionIdentifier visitFunctionName(SqlBaseParser.QualifiedNameContext ctx){

        List<String> identfiers = new ArrayList<>();
        for(SqlBaseParser.IdentifierContext identifierContext :ctx.identifier()){
            identfiers.add(identifierContext.getText());
        }

        if(identfiers.size()==2){
            String fn = identfiers.get(0);
            String db = identfiers.get(1);
            return new FunctionIdentifier(fn, db);
        }else if(identfiers.size()==1){
            String fn = identfiers.get(0);
            return new FunctionIdentifier(fn);
        }else{
            throw new ParseException("Unsupported function name '${ctx.getText}'", ctx);
        }


    }







}
