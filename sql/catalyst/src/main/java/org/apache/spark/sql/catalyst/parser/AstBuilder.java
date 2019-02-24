package org.apache.spark.sql.catalyst.parser;

import com.sun.org.apache.xpath.internal.functions.Function2Args;
import javafx.util.Pair;
import lombok.Data;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.RuleNode;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute;
import org.apache.spark.sql.catalyst.analysis.unresolved.*;
import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat;
import org.apache.spark.sql.catalyst.expressions.*;
import org.apache.spark.sql.catalyst.expressions.And;
import org.apache.spark.sql.catalyst.expressions.aggregate.First;
import org.apache.spark.sql.catalyst.expressions.aggregate.Last;
import org.apache.spark.sql.catalyst.expressions.arithmetic.*;
import org.apache.spark.sql.catalyst.expressions.bitwiseExpressions.BitwiseAnd;
import org.apache.spark.sql.catalyst.expressions.bitwiseExpressions.BitwiseNot;
import org.apache.spark.sql.catalyst.expressions.bitwiseExpressions.BitwiseOr;
import org.apache.spark.sql.catalyst.expressions.bitwiseExpressions.BitwiseXor;
import org.apache.spark.sql.catalyst.expressions.collectionOperations.Concat;
import org.apache.spark.sql.catalyst.expressions.complexTypeCreator.CreateNamedStruct;
import org.apache.spark.sql.catalyst.expressions.conditionalExpressions.CaseWhen;
import org.apache.spark.sql.catalyst.expressions.datetimeExpressions.*;
import org.apache.spark.sql.catalyst.expressions.grouping.Cube;
import org.apache.spark.sql.catalyst.expressions.grouping.Rollup;
import org.apache.spark.sql.catalyst.expressions.higherOrderFunctions.LambdaFunction;
import org.apache.spark.sql.catalyst.expressions.higherOrderFunctions.UnresolvedNamedLambdaVariable;
import org.apache.spark.sql.catalyst.expressions.literals.Literal;
import org.apache.spark.sql.catalyst.expressions.namedExpressions.Alias;
import org.apache.spark.sql.catalyst.expressions.namedExpressions.NamedExpression;
import org.apache.spark.sql.catalyst.expressions.nullExpressions.IsNotNull;
import org.apache.spark.sql.catalyst.expressions.nullExpressions.IsNull;
import org.apache.spark.sql.catalyst.expressions.predicates.*;
import org.apache.spark.sql.catalyst.expressions.regexpExpressions.Like;
import org.apache.spark.sql.catalyst.expressions.regexpExpressions.RLike;
import org.apache.spark.sql.catalyst.expressions.stringExpressions.StringLocate;
import org.apache.spark.sql.catalyst.expressions.subquery.Exists;
import org.apache.spark.sql.catalyst.expressions.subquery.ListQuery;
import org.apache.spark.sql.catalyst.expressions.subquery.ScalarSubquery;
import org.apache.spark.sql.catalyst.expressions.windowExpressions.*;
import org.apache.spark.sql.catalyst.identifiers.FunctionIdentifier;
import org.apache.spark.sql.catalyst.identifiers.TableIdentifier;
import org.apache.spark.sql.catalyst.plans.joinTypes.*;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.ScriptInputOutputSchema;
import org.apache.spark.sql.catalyst.plans.logical.ScriptTransformation;
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias;
import org.apache.spark.sql.catalyst.plans.logical.basicLogicalOperators.*;
import org.apache.spark.sql.catalyst.plans.logical.hints.UnresolvedHint;
import org.apache.spark.sql.catalyst.util.RandomSampler;
import org.apache.spark.sql.internal.SQLConf;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.spark.sql.catalyst.parser.ParserUtils;
import org.apache.spark.sql.types.*;

import java.sql.*;
import java.util.*;
import java.util.Date;
import java.util.function.Function;
import org.apache.spark.sql.catalyst.expressions.complexTypeCreator.CreateStruct;

import javax.annotation.PostConstruct;
import javax.xml.bind.DatatypeConverter;

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

                            LogicalPlan plan = withQuerySpecification(body.querySpecification(), from);
                            plan =ParserUtils.optionalMap(plan, body.queryOrganization(), (c,p)->{return withQueryResultClauses(c,p);});
                            plan =ParserUtils.optionalMap(plan, body.insertInto(), (c,p)->{return withInsertInto(c,p);});
                            inserts.add(plan);
                        }

                        if(inserts.size()==1){
                            return inserts.get(0);
                        }else{
                            return new Union(inserts);
                        }
                    }
                }
        );
    }

    @Override
    public LogicalPlan visitSingleInsertQuery(SqlBaseParser.SingleInsertQueryContext ctx){
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.SingleInsertQueryContext, LogicalPlan>() {
            @Override
            public LogicalPlan apply(SqlBaseParser.SingleInsertQueryContext ictx){
                LogicalPlan plan = plan(ictx.queryTerm());
                plan = withQueryResultClauses(ictx.queryOrganization(), plan);
                plan = withInsertInto(ictx.insertInto(), plan);
                return plan;
            }
        });
    }



    @Data
    public class InsertDirParams {
        boolean isLocal;
        CatalogStorageFormat storage;
        String provider;

        public InsertDirParams(boolean isLocal,
                               CatalogStorageFormat storage,
                               String provider) {
            this.isLocal = isLocal;
            this.storage = storage;
            this.provider = provider;
        }
    }

    @Data
    public class InsertTableParams {
        TableIdentifier tableIdent;
        Map<String, String> partitionKeys;
        boolean exists;

        public InsertTableParams(TableIdentifier tableIdent,
                                 Map<String, String> partitionKeys,
                                 boolean exists) {
            this.tableIdent = tableIdent;
            this.partitionKeys = partitionKeys;
            this.exists = exists;
        }
    }


    /**
     * Add an
     * {{{
     *   INSERT OVERWRITE TABLE tableIdentifier [partitionSpec [IF NOT EXISTS]]?
     *   INSERT INTO [TABLE] tableIdentifier [partitionSpec]
     *   INSERT OVERWRITE [LOCAL] DIRECTORY STRING [rowFormat] [createFileFormat]
     *   INSERT OVERWRITE [LOCAL] DIRECTORY [STRING] tableProvider [OPTIONS tablePropertyList]
     * }}}
     * operation to logical plan
     */
    private LogicalPlan withInsertInto(
            SqlBaseParser.InsertIntoContext ctx,
            LogicalPlan query) {
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.InsertIntoContext, LogicalPlan>() {
            @Override
            public LogicalPlan apply(SqlBaseParser.InsertIntoContext ictx) {

                if (ictx instanceof SqlBaseParser.InsertIntoTableContext) {
                    InsertTableParams insertTableParams = visitInsertIntoTable((SqlBaseParser.InsertIntoTableContext) ictx);
                    return new InsertIntoTable(
                            new UnresolvedRelation(insertTableParams.getTableIdent()),
                            insertTableParams.getPartitionKeys(),
                            query, false, insertTableParams.isExists());
                } else if (ictx instanceof SqlBaseParser.InsertOverwriteTableContext) {
                    InsertTableParams insertTableParams = visitInsertOverwriteTable((SqlBaseParser.InsertOverwriteTableContext) ictx);
                    return new InsertIntoTable(
                            new UnresolvedRelation(insertTableParams.getTableIdent()),
                            insertTableParams.getPartitionKeys(),
                            query, false, insertTableParams.isExists());
                } else if (ictx instanceof SqlBaseParser.InsertOverwriteDirContext) {
                    InsertDirParams insertDirParams = visitInsertOverwriteDir((SqlBaseParser.InsertOverwriteDirContext) ictx);
                    return new InsertIntoDir(insertDirParams.isLocal(), insertDirParams.getStorage(), insertDirParams.getProvider(), query, true);
                } else if (ictx instanceof SqlBaseParser.InsertOverwriteHiveDirContext) {
                    InsertDirParams insertDirParams = visitInsertOverwriteHiveDir((SqlBaseParser.InsertOverwriteHiveDirContext) ictx);
                    return new InsertIntoDir(insertDirParams.isLocal(), insertDirParams.getStorage(), insertDirParams.getProvider(), query, true);
                } else {
                    throw new ParseException("Invalid InsertIntoContext", ictx);
                }
            }

        });
    }



    @Override
    public InsertTableParams visitInsertIntoTable(
            SqlBaseParser.InsertIntoTableContext ctx) {
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.InsertIntoTableContext, InsertTableParams>() {
            @Override
            public InsertTableParams apply(SqlBaseParser.InsertIntoTableContext ictx) {
                TableIdentifier tableIdent = (TableIdentifier) visitTableIdentifier(ictx.tableIdentifier());
                Map<String, String> partitionKeys = visitPartitionSpec(ictx.partitionSpec());
                return new InsertTableParams(tableIdent, partitionKeys, false);
            }
        });
    }


    @Override
    public InsertTableParams visitInsertOverwriteTable(
            SqlBaseParser.InsertOverwriteTableContext ctx) {
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.InsertOverwriteTableContext, InsertTableParams>() {
            @Override
            public InsertTableParams apply(SqlBaseParser.InsertOverwriteTableContext ictx) {
                assert (ictx.OVERWRITE() != null);
                TableIdentifier tableIdent = (TableIdentifier) visitTableIdentifier(ictx.tableIdentifier());
                Map<String, String> partitionKeys = visitPartitionSpec(ictx.partitionSpec());
                int dynamicPartitionKeys = 0;
                for (Map.Entry<String, String> entry : partitionKeys.entrySet()) {
                    if (entry.getValue() == null) {
                        dynamicPartitionKeys++;
                    }
                }
                //dynamicPartitionKeys: Map[String, Option[String]] = partitionKeys.filter(_._2.isEmpty)
                if (ictx.EXISTS() != null && dynamicPartitionKeys > 0) {
                    throw new ParseException("Dynamic partitions do not support IF NOT EXISTS. Specified " +
                            "partitions with value: ]", ictx);
                }
                return new InsertTableParams(tableIdent, partitionKeys, ictx.EXISTS() != null);
            }
        });
    }


    @Override
    public InsertDirParams visitInsertOverwriteDir(
            SqlBaseParser.InsertOverwriteDirContext ctx) {
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.InsertOverwriteDirContext, InsertDirParams>() {
            @Override
            public InsertDirParams apply(SqlBaseParser.InsertOverwriteDirContext ictx) {
                throw new ParseException("INSERT OVERWRITE DIRECTORY is not supported", ictx);
            }
        });

    }

    @Override
    public InsertDirParams visitInsertOverwriteHiveDir(
            SqlBaseParser.InsertOverwriteHiveDirContext ctx){
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.InsertOverwriteHiveDirContext, InsertDirParams>() {
            @Override
            public InsertDirParams apply(SqlBaseParser.InsertOverwriteHiveDirContext ictx) {
                throw new ParseException("INSERT OVERWRITE DIRECTORY is not supported", ictx);
            }
        });

    }

    @Override
    public Map<String, String>visitPartitionSpec(
            SqlBaseParser.PartitionSpecContext ctx) {
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.PartitionSpecContext, Map<String, String>>() {
            @Override
            public Map<String, String> apply(SqlBaseParser.PartitionSpecContext qctx) {

                List<Pair<String, String>> parts = ParserUtils.map(qctx.partitionVal(), new Function<SqlBaseParser.PartitionValContext, Pair<String, String>>() {
                    @Override
                    public Pair<String, String> apply(SqlBaseParser.PartitionValContext pVal) {
                        String name = pVal.identifier().getText();
                        String value = visitStringConstant(pVal.constant());
                        return new Pair<>(name, value);
                    }
                });

                ParserUtils.checkDuplicateKeys(parts, qctx);

                return ParserUtils.toMap(parts);
            }
        });
    }

    protected Map<String,String> visitNonOptionalPartitionSpec(
            SqlBaseParser.PartitionSpecContext ctx) {
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.PartitionSpecContext, Map<String, String>>() {
            @Override
            public Map<String, String> apply(SqlBaseParser.PartitionSpecContext pctx) {
                Map<String, String> map = visitPartitionSpec(pctx);
                for (Map.Entry<String, String> entry : map.entrySet()) {
                    if (entry.getValue() == null) {
                        throw new ParseException("Found an empty partition key '$key'.", ctx);
                    }
                }
                return map;
            }
        });
    }


    protected String visitStringConstant(SqlBaseParser.ConstantContext ctx){
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.ConstantContext, String>() {
                    @Override
                    public String apply(SqlBaseParser.ConstantContext constantContext) {
                        if(constantContext instanceof SqlBaseParser.StringLiteralContext){
                            return createString((SqlBaseParser.StringLiteralContext)constantContext);
                        }else{
                            return constantContext.getText();
                        }
                    }
                });
        }




    /**
     * Add ORDER BY/SORT BY/CLUSTER BY/DISTRIBUTE BY/LIMIT/WINDOWS clauses to the logical plan. These
     * clauses determine the shape (ordering/partitioning/rows) of the query result.
     */
    private LogicalPlan withQueryResultClauses(
            SqlBaseParser.QueryOrganizationContext ctx,
            LogicalPlan query){
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.QueryOrganizationContext, LogicalPlan>() {
            @Override
            public LogicalPlan apply(SqlBaseParser.QueryOrganizationContext qctx){

                LogicalPlan withOrder;
                List<SqlBaseParser.SortItemContext> order = qctx.order;
                List<SqlBaseParser.SortItemContext> sort = qctx.sort;
                List<SqlBaseParser.ExpressionContext> distributeBy = qctx.distributeBy;
                List<SqlBaseParser.ExpressionContext> clusterBy = qctx.clusterBy;
                if(order.size()>0 && sort.size()==0 && distributeBy.size()==0 && clusterBy.size()==0){
                    List<SortOrder> sortOrders = ParserUtils.map(order, new Function<SqlBaseParser.SortItemContext, SortOrder>(){
                        @Override
                        public SortOrder apply(SqlBaseParser.SortItemContext context){
                            return visitSortItem(context);
                        }
                    });
                    withOrder = new Sort(sortOrders, true,query);
                } else if (order.size()==0 && sort.size()>0 && distributeBy.size()==0 && clusterBy.size()==0) {
                    List<SortOrder> sortOrders = ParserUtils.map(sort, new Function<SqlBaseParser.SortItemContext, SortOrder>(){
                        @Override
                        public SortOrder apply(SqlBaseParser.SortItemContext context){
                            return visitSortItem(context);
                        }
                    });
                    withOrder = new Sort(sortOrders, false, query);
                } else if (order.size()==0 && sort.size()==0 && distributeBy.size()>0 && clusterBy.size()==0) {
                    // DISTRIBUTE BY ...
                    withOrder = withRepartitionByExpression(qctx, expressionList(distributeBy), query);
                } else if (order.size()==0 && sort.size()>0 && distributeBy.size()>0 && clusterBy.size()==0) {
                    // SORT BY ... DISTRIBUTE BY ...
                    List<SortOrder> sortOrders = ParserUtils.map(sort, new Function<SqlBaseParser.SortItemContext, SortOrder>(){
                        @Override
                        public SortOrder apply(SqlBaseParser.SortItemContext context){
                            return visitSortItem(context);
                        }
                    });
                    withOrder = new Sort(
                            sortOrders,
                            false,
                            withRepartitionByExpression(qctx, expressionList(distributeBy), query));
                } else if (order.size()==0 && sort.size()==0 && distributeBy.size()==0 && clusterBy.size()>0) {
                    // CLUSTER BY ...
                    List<Expression> expressions = expressionList(clusterBy);
                    List<SortOrder> sortOrders = ParserUtils.map(expressions, new Function<Expression, SortOrder>(){
                        @Override
                        public SortOrder apply(Expression expression){
                            return new SortOrder(expression, new Ascending());
                        }
                    });
                    withOrder = new Sort(
                            sortOrders,
                            false,
                            withRepartitionByExpression(qctx, expressions, query));
                } else if (order.size()==0 && sort.size()==0 && distributeBy.size()==0 && clusterBy.size()==0) {
                    // [EMPTY]
                    withOrder = query;
                } else {
                    throw new ParseException(
                            "Combination of ORDER BY/SORT BY/DISTRIBUTE BY/CLUSTER BY is not supported", qctx);
                }

                LogicalPlan withWindow = ParserUtils.optionalMap(withOrder,qctx.windows(), (c,p)->{
                    return withWindows(c,p);
                });

                return ParserUtils.optional(withWindow,qctx.limit,(c,p)->{
                    return Limit.build(typedVisit((ParseTree)c),p);
                });
            }
        });
    }


    protected LogicalPlan withRepartitionByExpression(
            SqlBaseParser.QueryOrganizationContext ctx,
            List<Expression>expressions,
            LogicalPlan query){
        throw new ParseException("DISTRIBUTE BY is not supported", ctx);
    }


    @Override
    public LogicalPlan visitQuerySpecification(
            SqlBaseParser.QuerySpecificationContext ctx) {
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.QuerySpecificationContext, LogicalPlan>() {
            @Override
            public LogicalPlan apply(SqlBaseParser.QuerySpecificationContext qctx) {

                LogicalPlan from = ParserUtils.optional(new OneRowRelation(), qctx.fromClause(), (c, p) -> {
                    return visitFromClause(qctx.fromClause());
                });
                return withQuerySpecification(qctx, from);
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



                        LogicalPlan withWindow = ParserUtils.optionalMap(withFilter, querySpecificationContext.windows(),(c,q)->{
                            return withWindows(c,q);
                        });

                        return ParserUtils.foldRight(querySpecificationContext.hints,withWindow, (p,c)->{
                            return withHints(c,p);
                        });
                    }
                }
                return null;
            }
        });
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
                }else{
                    return ParserUtils.foldLeft(fromClauseContext.lateralView(),from, (c,p)->{
                        return withGenerate(c,p);
                    });
                }
            }
        });
    }

    @Override
    public LogicalPlan visitSetOperation(SqlBaseParser.SetOperationContext ctx){
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.SetOperationContext, LogicalPlan>() {
            @Override
            public LogicalPlan apply(SqlBaseParser.SetOperationContext sctx){
                LogicalPlan left = plan(sctx.left);
                LogicalPlan right = plan(sctx.right);
                boolean all = sctx.setQuantifier().ALL() != null;
                switch(sctx.operator.getType()){
                    case SqlBaseParser.UNION:
                        if(all){
                            return new Union(left, right);
                        }else{
                            return new Distinct(new Union(left, right));
                        }
                    case SqlBaseParser.INTERSECT:
                        if(all){
                            return new Intersect(left, right, true);
                        }else{
                            return new Intersect(left, right, false);
                        }
                    case SqlBaseParser.EXCEPT:
                        if(all){
                            return new Except(left, right, true);
                        }else{
                            return new Except(left, right, false);
                        }
                    case SqlBaseParser.SETMINUS:
                        if(all){
                            return new Except(left, right, true);
                        }else{
                            return new Except(left, right, false);
                        }
                }
                return null;
            }
        });
    }



    /**
     * Add a [[WithWindowDefinition]] operator to a logical plan.
     */
    private LogicalPlan withWindows(
            SqlBaseParser.WindowsContext ctx,
            LogicalPlan query) {
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.WindowsContext, LogicalPlan>() {
            @Override
            public LogicalPlan apply(SqlBaseParser.WindowsContext wctx) {

                Map<String, WindowSpec> baseWindowMap = new HashMap<>();
                for (SqlBaseParser.NamedWindowContext namedWindowContext : wctx.namedWindow()) {
                    baseWindowMap.put(namedWindowContext.identifier().getText(), typedVisit(namedWindowContext.windowSpec()));
                }

                List<WindowSpecDefinition> windowMapView = new ArrayList<>();
                for (WindowSpec windowSpec : baseWindowMap.values()) {
                    if (windowSpec instanceof WindowSpecReference) {
                        String name = ((WindowSpecReference) windowSpec).getName();
                        WindowSpec spec = baseWindowMap.get(name);
                        if (spec instanceof WindowSpecDefinition) {
                            windowMapView.add((WindowSpecDefinition) spec);
                        } else if (spec == null) {
                            throw new ParseException("Cannot resolve window reference '$name'", ctx);
                        } else {
                            throw new ParseException("Window reference '$name' is not a window specification", ctx);
                        }
                    } else if (windowSpec instanceof WindowSpecDefinition) {
                        windowMapView.add((WindowSpecDefinition) windowSpec);
                    }
                }

//                TODO:
//                Map<String ,WindowSpecDefinition>windowDefinitions = new HashMap<>();
//                for(WindowSpec entry : windowMapView){
//                    windowDefinitions.put(entry.)
//                }
//                return new WithWindowDefinition(windowMapView, query);
                return new WithWindowDefinition(null, query);
            }
        });
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


    private LogicalPlan withHints(
            SqlBaseParser.HintContext ctx,
            LogicalPlan query){
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.HintContext, LogicalPlan>() {
            @Override
            public LogicalPlan apply(SqlBaseParser.HintContext hintContext){
                LogicalPlan plan = query;
                List<SqlBaseParser.HintStatementContext> hintStatements = hintContext.hintStatements;
                Collections.reverse(hintStatements);

                for(SqlBaseParser.HintStatementContext stmt: hintStatements){
                    if(stmt==null){
                        continue;
                    }
                    List<Object> objects = new ArrayList<>();
                    for(SqlBaseParser.PrimaryExpressionContext primary:stmt.parameters){
                        objects.add(expression(primary));
                    }
                    plan = new UnresolvedHint(stmt.hintName.getText(),objects , plan);

                }

                return plan;
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
            }
        });
    }

    @Override
    public LogicalPlan visitRelation(SqlBaseParser.RelationContext ctx) {
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.RelationContext, LogicalPlan>() {
            @Override
            public LogicalPlan apply(SqlBaseParser.RelationContext rctx) {
                return withJoinRelations(plan(rctx.relationPrimary()), rctx);
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
                            joinType = new UsingJoin(baseJoinType, visitIdentifierList(criteria.identifierList()));
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

    private LogicalPlan withSample(SqlBaseParser.SampleContext ctx, LogicalPlan query){
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.SampleContext, LogicalPlan>() {



            @Override
            public LogicalPlan apply(SqlBaseParser.SampleContext sctx){

                Function<Double, Sample>sample = new Function<Double, Sample>() {
                    @Override
                    public Sample apply(Double fraction) {
                        Double eps = RandomSampler.roundingEpsilon;
                        ParserUtils.validate(fraction >= 0.0 - eps && fraction <= 1.0 + eps,
                                "Sampling fraction ($fraction) must be on interval [0, 1]",
                                sctx);
                        return new Sample(0.0, fraction, false, Math.round(Math.random()* 1000), query);
                    }
                };


                if (sctx.sampleMethod() == null) {
                    throw new ParseException("TABLESAMPLE does not accept empty inputs.", sctx);
                }

                SqlBaseParser.SampleMethodContext sampleMethod = sctx.sampleMethod();

                if(sampleMethod instanceof SqlBaseParser.SampleByRowsContext){
                    SqlBaseParser.SampleByRowsContext sm =  (SqlBaseParser.SampleByRowsContext)sampleMethod;
                    return Limit.build(expression(sm.expression()), query);
                }else if(sampleMethod instanceof SqlBaseParser.SampleByPercentileContext){
                    SqlBaseParser.SampleByPercentileContext sm =  (SqlBaseParser.SampleByPercentileContext)sampleMethod;
                    Double fraction = Double.valueOf(sm.percentage.getText());
                    int sign = 1;
                    if (sm.negativeSign != null) {
                        sign = -1;
                    }
                    return sample.apply(sign * fraction / 100.0d);
                }else if(sampleMethod instanceof SqlBaseParser.SampleByBytesContext){
                    SqlBaseParser.SampleByBytesContext sm =  (SqlBaseParser.SampleByBytesContext)sampleMethod;
                    String bytesStr = sm.bytes.getText();
                    if (bytesStr.matches("[0-9]+[bBkKmMgG]")) {
                        throw new ParseException("TABLESAMPLE(byteLengthLiteral) is not supported", sctx);
                    } else {
                        throw new ParseException(
                                bytesStr + " is not a valid byte length literal, " +
                                        "expected syntax: DIGIT+ ('B' | 'K' | 'M' | 'G')", sctx);
                    }
                }else if(sampleMethod instanceof SqlBaseParser.SampleByBucketContext){
                    SqlBaseParser.SampleByBucketContext sm =  (SqlBaseParser.SampleByBucketContext)sampleMethod;
                    if(sm.ON()!=null){
                        if (sm.identifier() != null) {
                            throw new ParseException(
                                    "TABLESAMPLE(BUCKET x OUT OF y ON colname) is not supported", sm);
                        } else {
                            throw new ParseException(
                                    "TABLESAMPLE(BUCKET x OUT OF y ON function) is not supported", sm);
                        }
                    }else{
                        return sample.apply(Double.valueOf(sm.numerator.getText()) / Double.valueOf(sm.denominator.getText()));
                    }
                }
                return null;
            }
        });
    }



    @Override
    public LogicalPlan visitSubquery(SqlBaseParser.SubqueryContext ctx){
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.SubqueryContext, LogicalPlan>() {
            @Override
            public LogicalPlan apply(SqlBaseParser.SubqueryContext sctx){
                return plan(sctx.queryNoWith());
            }
        });
    }




    @Override
    public LogicalPlan visitTable(SqlBaseParser.TableContext ctx){
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.TableContext, LogicalPlan>() {
            @Override
            public LogicalPlan apply(SqlBaseParser.TableContext tctx){
                return new UnresolvedRelation(visitTableIdentifier(tctx.tableIdentifier()));
            }
        });

    }


    @Override
    public LogicalPlan visitTableName(SqlBaseParser.TableNameContext ctx){
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.TableNameContext, LogicalPlan>() {
            @Override
            public LogicalPlan apply(SqlBaseParser.TableNameContext tctx){
                TableIdentifier tableId = visitTableIdentifier(tctx.tableIdentifier());
                LogicalPlan table = mayApplyAliasPlan(tctx.tableAlias(), new UnresolvedRelation(tableId));
                return ParserUtils.optionalMap(table,tctx.sample(),(c,p)->{
                    return withSample(c,p);
                });
            }
        });
    }


    @Override
    public LogicalPlan visitTableValuedFunction(SqlBaseParser.TableValuedFunctionContext ctx) {
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.TableValuedFunctionContext, LogicalPlan>() {
            @Override
            public LogicalPlan apply(SqlBaseParser.TableValuedFunctionContext tctx) {
                SqlBaseParser.FunctionTableContext func = tctx.functionTable();
                List<String> aliases = new ArrayList<>();
                if (func.tableAlias().identifierList() != null) {
                    aliases = visitIdentifierList(func.tableAlias().identifierList());
                }
                UnresolvedTableValuedFunction tvf = new UnresolvedTableValuedFunction(
                        func.identifier().getText(),
                        ParserUtils.map(func.expression(), new Function<SqlBaseParser.ExpressionContext, Expression>() {
                            @Override
                            public Expression apply(SqlBaseParser.ExpressionContext expressionContext) {
                                return expression(expressionContext);
                            }
                        }),
                        aliases
                );
                return ParserUtils.optionalMap(tvf,func.tableAlias().strictIdentifier(), (c,p)->{
                    return aliasPlan(c,p);
                } );
            }
        });
    }



    @Override
    public LogicalPlan visitInlineTable(SqlBaseParser.InlineTableContext ctx) {
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.InlineTableContext, LogicalPlan>() {
            @Override
            public LogicalPlan apply(SqlBaseParser.InlineTableContext ictx) {

                List<List<Expression>> rows = ParserUtils.map(ictx.expression(), new Function<SqlBaseParser.ExpressionContext, List<Expression>>() {
                    @Override
                    public List<Expression> apply(SqlBaseParser.ExpressionContext ectx) {
                        List<Expression> expressions = new ArrayList<>();
                        Expression e = expression(ectx);
                        if (e instanceof CreateNamedStruct) {
                            CreateNamedStruct struct = (CreateNamedStruct) e;
                            expressions = struct.valExprs();
                        } else {
                            expressions.add(e);
                        }
                        return expressions;
                    }
                });

                List<String> aliases;
                if (ictx.tableAlias().identifierList() != null) {
                    aliases = visitIdentifierList(ictx.tableAlias().identifierList());
                } else {
                    aliases = new ArrayList<>();
                    for (int i = 0; i < rows.size(); i++) {
                        aliases.add("col$" + (i + 1));
                    }
                }
                UnresolvedInlineTable table = new UnresolvedInlineTable(aliases, rows);
                return ParserUtils.optionalMap(table, ictx.tableAlias().strictIdentifier(), (c, p) -> {
                    return aliasPlan(c, p);
                });
            }
        });
    }


    @Override
    public LogicalPlan visitAliasedRelation(SqlBaseParser.AliasedRelationContext ctx){
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.AliasedRelationContext, LogicalPlan>() {
            @Override
            public LogicalPlan apply(SqlBaseParser.AliasedRelationContext actx){
                LogicalPlan relation = ParserUtils.optionalMap(plan(actx.relation()), actx.sample(),(c,p)->{
                    return withSample(c,p);
                });
                return mayApplyAliasPlan(actx.tableAlias(), relation);
            }
        });
    }

    @Override
    public LogicalPlan visitAliasedQuery(SqlBaseParser.AliasedQueryContext ctx){
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.AliasedQueryContext, LogicalPlan>() {
            @Override
            public LogicalPlan apply(SqlBaseParser.AliasedQueryContext actx){
                LogicalPlan relation = ParserUtils.optionalMap(
                        plan(actx.queryNoWith()),
                                actx.sample(),
                        (c,p)->{return withSample(c,p);});
                if(actx.tableAlias().strictIdentifier()==null){
                    return new SubqueryAlias("__auto_generated_subquery_name", relation);
                }else{
                    return mayApplyAliasPlan(actx.tableAlias(), relation);
                }
            }
        });
    }


    private LogicalPlan aliasPlan(ParserRuleContext alias, LogicalPlan plan){
        return new SubqueryAlias(alias.getText(), plan);
    }

    private LogicalPlan mayApplyAliasPlan(SqlBaseParser.TableAliasContext tableAlias, LogicalPlan plan){
        if (tableAlias.strictIdentifier() != null) {
            SubqueryAlias subquery = new SubqueryAlias(tableAlias.strictIdentifier().getText(), plan);
            if (tableAlias.identifierList() != null) {
                List<String> columnNames = visitIdentifierList(tableAlias.identifierList());
                return new UnresolvedSubqueryColumnAliases(columnNames, subquery);
            } else {
                return subquery;
            }
        } else {
            return plan;
        }
    }
    @Override
    public List<String> visitIdentifierList(SqlBaseParser.IdentifierListContext ctx){
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.IdentifierListContext, List<String>>() {
            @Override
            public List<String> apply(SqlBaseParser.IdentifierListContext ictx){
                return visitIdentifierSeq(ictx.identifierSeq());
            }
        });
    }

    @Override
    public TableIdentifier visitTableIdentifier(SqlBaseParser.TableIdentifierContext ctx){
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.TableIdentifierContext, TableIdentifier>() {
            @Override
            public TableIdentifier apply(SqlBaseParser.TableIdentifierContext tctx){
                return new TableIdentifier(tctx.table.getText(), tctx.db.getText());
            }
        });

    }

    @Override
    public FunctionIdentifier visitFunctionIdentifier(SqlBaseParser.FunctionIdentifierContext ctx){
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.FunctionIdentifierContext, FunctionIdentifier>() {
            @Override
            public FunctionIdentifier apply(SqlBaseParser.FunctionIdentifierContext fctx){
                return new FunctionIdentifier(fctx.function.getText(), fctx.db.getText());
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

    @Override
    public Expression visitStar(SqlBaseParser.StarContext ctx){
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.StarContext, Expression>() {
            @Override
            public Expression apply(SqlBaseParser.StarContext sctx){
                return new UnresolvedStar(ParserUtils.map(sctx.qualifiedName().identifier(), new Function<SqlBaseParser.IdentifierContext, String>() {
                    @Override
                    public String apply(SqlBaseParser.IdentifierContext c) {
                        return c.getText();
                    }
                }));
            }
        });
    }


    @Override
    public Expression visitNamedExpression(SqlBaseParser.NamedExpressionContext ctx){
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.NamedExpressionContext, Expression>() {
            @Override
            public Expression apply(SqlBaseParser.NamedExpressionContext nctx){
                Expression e = expression(nctx.expression());
                if (nctx.identifier() != null) {
                    //TODO:
                    return new Alias(e, nctx.identifier().getText());
                } else if (nctx.identifierList() != null) {
                    return new MultiAlias(e, visitIdentifierList(nctx.identifierList()));
                } else {
                    return e;
                }
            }
        });
    }


    //very trick
    @Override
    public Expression visitLogicalBinary(SqlBaseParser.LogicalBinaryContext ctx){
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.LogicalBinaryContext, Expression>() {
            @Override
            public Expression apply(SqlBaseParser.LogicalBinaryContext lctx){
                int expressionType = lctx.operator.getType();
                Function<Pair<Expression,Expression>, Expression>expressionCombiner;
                if(expressionType==SqlBaseParser.AND) {
                    expressionCombiner=new Function<Pair<Expression, Expression>, Expression>() {
                        @Override
                        public Expression apply(Pair<Expression, Expression> pair) {
                            return new And(pair.getKey(), pair.getValue());
                        }
                    };
                }else if(expressionType==SqlBaseParser.OR) {
                    expressionCombiner=new Function<Pair<Expression, Expression>, Expression>() {
                        @Override
                        public Expression apply(Pair<Expression, Expression> pair) {
                            return new Or(pair.getKey(), pair.getValue());
                        }
                    };
                }else{
                    expressionCombiner = null;
                }

                List<SqlBaseParser.BooleanExpressionContext> contexts = new ArrayList<>();
                contexts.add(lctx.right);
                SqlBaseParser.BooleanExpressionContext current = lctx.left;

                while(true){
                    SqlBaseParser.LogicalBinaryContext lbc = (SqlBaseParser.LogicalBinaryContext)current;
                    if(lbc.operator.getType()==expressionType){
                        contexts.add(lbc.right);
                        current = lbc.left;
                    }else{
                        contexts.add(current);
                        break;
                    }
                }
                List<Expression> expressions = ParserUtils.reverseMap(contexts, new Function<SqlBaseParser.BooleanExpressionContext, Expression>() {
                    @Override
                    public Expression apply(SqlBaseParser.BooleanExpressionContext bctx){
                        return expression(bctx);
                    }
                });

                return reduceToExpressionTree(0,expressions.size()-1, expressions, expressionCombiner);
            }
        });
    }


    private Expression reduceToExpressionTree(int low, int high, List<Expression> expressions,Function<Pair<Expression,Expression>, Expression>expressionCombiner) {
        int off = high - low;
        if (off == 0) {
            return expressions.get(low);
        } else if (off == 1) {
            return expressionCombiner.apply(new Pair<>(expressions.get(low), expressions.get(high)));
        } else {
            int mid = low + off / 2;
            return expressionCombiner.apply(new Pair<>(
                    reduceToExpressionTree(low, mid, expressions, expressionCombiner),
                    reduceToExpressionTree(mid + 1, high, expressions, expressionCombiner)));
        }
    }




    @Override
    public Expression visitLogicalNot(SqlBaseParser.LogicalNotContext ctx){
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.LogicalNotContext, Expression>() {
            @Override
            public Expression apply(SqlBaseParser.LogicalNotContext lctx){
                return new Not(expression(lctx.booleanExpression()));
            }
        });
    }


    @Override
    public Expression visitExists(SqlBaseParser.ExistsContext ctx){
        return new Exists(plan(ctx.query()));
    }

    @Override
    public Expression visitComparison(SqlBaseParser.ComparisonContext ctx){
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.ComparisonContext, Expression>() {
            @Override
            public Expression apply(SqlBaseParser.ComparisonContext cctx){
                Expression left = expression(cctx.left);
                Expression right = expression(cctx.right);
                TerminalNode operator = (TerminalNode)cctx.comparisonOperator().getChild(0);
                switch(operator.getSymbol().getType()){
                    case SqlBaseParser.EQ:
                        return new EqualTo(left, right);
                    case SqlBaseParser.NSEQ:
                        return new EqualNullSafe(left, right);
                    case SqlBaseParser.NEQ:
                    case  SqlBaseParser.NEQJ:
                        return new Not(new EqualTo(left, right));
                    case SqlBaseParser.LT:
                        return new LessThan(left, right);
                    case SqlBaseParser.LTE:
                        return new LessThanOrEqual(left, right);
                    case SqlBaseParser.GT:
                        return new GreaterThan(left, right);
                    case SqlBaseParser.GTE:
                        return new GreaterThanOrEqual(left, right);
                }
                return null;
            }
        });
    }


    @Override
    public Expression visitPredicated(SqlBaseParser.PredicatedContext ctx){
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.PredicatedContext, Expression>() {
            @Override
            public Expression apply(SqlBaseParser.PredicatedContext pctx){
                Expression e = expression(pctx.valueExpression());
                if(pctx.predicate()!=null){
                    return withPredicate(e, pctx.predicate());
                }else{
                    return e;
                }
            }
        });
    }

    private Expression withPredicate(Expression e, SqlBaseParser.PredicateContext ctx){
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.PredicateContext, Expression>() {
            @Override
            public Expression apply(SqlBaseParser.PredicateContext pctx){
                Function<Expression,Expression>invertIfNotDefined = new Function<Expression, Expression>() {
                    @Override
                    public Expression apply(Expression expression) {
                        if(pctx.NOT()==null){
                            return expression;
                        }else{
                            return new Not(expression);
                        }
                    }
                };
                Function<Expression,List<Expression>> getValueExpressions = new Function<Expression, List<Expression>>() {
                    @Override
                    public List<Expression> apply(Expression expression) {
                        if(expression instanceof CreateNamedStruct){
                            CreateNamedStruct c = (CreateNamedStruct)expression;
                            return c.valExprs();
                        }else{
                            return ParserUtils.Seq(expression);
                        }
                    }
                };

                switch (pctx.kind.getType()){
                    case SqlBaseParser.BETWEEN :
                        // BETWEEN is translated to lower <= e && e <= upper
                        return invertIfNotDefined.apply(new And(
                                new GreaterThanOrEqual(e, expression(pctx.lower)),
                                new LessThanOrEqual(e, expression(pctx.upper))));
                    case SqlBaseParser.IN:
                        if(pctx.query()!= null){
                            return invertIfNotDefined.apply(new InSubquery(getValueExpressions.apply(e), new ListQuery(plan(pctx.query()))));
                        }else{
                            return invertIfNotDefined.apply(new In(e, ParserUtils.map(pctx.expression(), new Function<SqlBaseParser.ExpressionContext, Expression>() {
                                @Override
                                public Expression apply(SqlBaseParser.ExpressionContext ectx){
                                    return expression(ectx);
                                }
                            })));
                        }
                    case SqlBaseParser.LIKE:
                        return invertIfNotDefined.apply(new Like(e, expression(pctx.pattern)));
                    case SqlBaseParser.RLIKE:
                        return invertIfNotDefined.apply(new RLike(e, expression(pctx.pattern)));
                    case SqlBaseParser.NULL:
                        if(pctx.NOT() != null) {
                            return new IsNotNull(e);
                        }else{
                            return new IsNull(e);
                        }
                    case SqlBaseParser.DISTINCT:
                        if(pctx.NOT() != null) {
                            return new EqualNullSafe(e, expression(pctx.right));
                        }else {
                            return new Not(new EqualNullSafe(e, expression(pctx.right)));
                        }
                }
                return null;
            }
        });
    }








    @Override
    public Expression visitArithmeticBinary(SqlBaseParser.ArithmeticBinaryContext ctx){
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.ArithmeticBinaryContext, Expression>() {
            @Override
            public Expression apply(SqlBaseParser.ArithmeticBinaryContext actx){
                Expression left = expression(actx.left);
                Expression right = expression(actx.right);
                switch (actx.operator.getType()){
                    case SqlBaseParser.ASTERISK:
                        return new Multiply(left, right);
                    case SqlBaseParser.SLASH:
                        return new Divide(left, right);
                    case SqlBaseParser.PERCENT:
                        return new Remainder(left, right);
                    case SqlBaseParser.DIV:
                        return new IntegralDivide(left, right);
                    case SqlBaseParser.PLUS:
                        return new Add(left, right);
                    case SqlBaseParser.MINUS:
                        return new Subtract(left, right);
                    case SqlBaseParser.CONCAT_PIPE:
                        return new Concat(ParserUtils.Seq(left,right));
                    case SqlBaseParser.AMPERSAND:
                        return new BitwiseAnd(left, right);
                    case SqlBaseParser.HAT:
                        return new BitwiseXor(left, right);
                    case SqlBaseParser.PIPE:
                        return new BitwiseOr(left, right);
                }
                return null;
            }
        });
    }


    @Override
    public Expression visitArithmeticUnary(SqlBaseParser.ArithmeticUnaryContext ctx){
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.ArithmeticUnaryContext, Expression>() {
            @Override
            public Expression apply(SqlBaseParser.ArithmeticUnaryContext actx){
                Expression value = expression(actx.valueExpression());
                switch (actx.operator.getType()){
                    case SqlBaseParser.PLUS:
                        return value;
                    case SqlBaseParser.MINUS:
                        return new UnaryMinus(value);
                    case SqlBaseParser.TILDE:
                        return new BitwiseNot(value);
                }
                return null;
            }
        });
    }


    @Override
    public Expression visitCast(SqlBaseParser.CastContext ctx){
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.CastContext, Expression>() {
            @Override
            public Expression apply(SqlBaseParser.CastContext cctx){
                return  new Cast(expression(cctx.expression()), visitSparkDataType(cctx.dataType()));
            }
        });
    }

    @Override
    public Expression visitStruct(SqlBaseParser.StructContext ctx){
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.StructContext, Expression>() {
            @Override
            public Expression apply(SqlBaseParser.StructContext sctx){
                CreateStruct createStruct = new CreateStruct();
                return  createStruct.apply( ParserUtils.map(sctx.argument, new Function<SqlBaseParser.NamedExpressionContext,Expression>(){
                    @Override
                    public Expression apply(SqlBaseParser.NamedExpressionContext c) {
                        return expression(c);
                    }
                }));
            }
        });
    }

    @Override
    public Expression visitFirst(SqlBaseParser.FirstContext ctx){
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.FirstContext, Expression>() {
            @Override
            public Expression apply(SqlBaseParser.FirstContext fctx){
                boolean ignoreNullsExpr = fctx.IGNORE() != null;
                return new First(expression(fctx.expression()), Literal.build(ignoreNullsExpr)).toAggregateExpression();
            }
        });
    }

    @Override
    public Expression visitLast(SqlBaseParser.LastContext ctx){
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.LastContext, Expression>() {
            @Override
            public Expression apply(SqlBaseParser.LastContext fctx){
                boolean ignoreNullsExpr = fctx.IGNORE() != null;
                return new Last(expression(fctx.expression()), Literal.build(ignoreNullsExpr)).toAggregateExpression();
            }
        });
    }

    @Override
    public Expression visitPosition(SqlBaseParser.PositionContext ctx){
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.PositionContext, Expression>() {
            @Override
            public Expression apply(SqlBaseParser.PositionContext pctx){
                return new StringLocate(expression(pctx.substr), expression(pctx.str));
            }
        });
    }


    @Override
    public Expression visitExtract(SqlBaseParser.ExtractContext ctx){
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.ExtractContext, Expression>() {
            @Override
            public Expression apply(SqlBaseParser.ExtractContext ectx){
                switch (ectx.field.getText().toUpperCase(Locale.ROOT)){
                    case "YEAR":
                        return new Year(expression(ectx.source));
                    case "QUARTER":
                        return new Quarter(expression(ectx.source));
                    case "MONTH":
                        return new Month(expression(ectx.source));
                    case "WEEK":
                        return new WeekOfYear(expression(ectx.source));
                    case "DAY" :
                        return new DayOfMonth(expression(ectx.source));
                    case "DAYOFWEEK":
                        return new DayOfWeek(expression(ectx.source));
                    case "HOUR":
                        return new Hour(expression(ectx.source));
                    case "MINUTE":
                        return new Minute(expression(ectx.source));
                    case "SECOND":
                        return new Second(expression(ectx.source));
                    default:
                        throw new ParseException("Literals of type '$other' are currently not supported.", ectx);
                }
            }
        });
    }


    private  FunctionIdentifier replaceFunctions(
            FunctionIdentifier funcID,
            SqlBaseParser.FunctionCallContext ctx){
        Token opt = ctx.trimOption;
        if (opt != null) {
            if (ctx.qualifiedName().getText().toLowerCase(Locale.ROOT) != "trim") {
                throw new ParseException("The specified function ${ctx.qualifiedName.getText} " +
                        "doesn't support with option ${opt.getText}.", ctx);
            }
            switch (opt.getType()){
            case SqlBaseParser.BOTH:
                return funcID;
                case SqlBaseParser.LEADING:
                    return new FunctionIdentifier("ltrim", funcID.getDatabase());
                case SqlBaseParser.TRAILING:
                    return new FunctionIdentifier("rtrim", funcID.getDatabase());
                default: throw new ParseException("Function trim doesn't support with " +
                        "type ${opt.getType}. Please use BOTH, LEADING or Trailing as trim type", ctx);
            }
        } else {
            return funcID;
        }
    }

    @Override
    public Expression visitFunctionCall(SqlBaseParser.FunctionCallContext ctx){
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.FunctionCallContext, Expression>() {
            @Override
            public Expression apply(SqlBaseParser.FunctionCallContext fctx) {

                String  name = fctx.qualifiedName().getText();
                boolean isDistinct = fctx.setQuantifier().DISTINCT()!=null;

                //List<Expression>
                List<Expression> arguments = ParserUtils.map(fctx.argument, new Function<SqlBaseParser.ExpressionContext, Expression>() {
                            @Override
                            public Expression apply(SqlBaseParser.ExpressionContext ectx){
                                return expression(ectx);
                            }
                        });

                if(arguments.size()==1 && arguments.get(0) instanceof UnresolvedStar){
                    UnresolvedStar unresolvedStar = (UnresolvedStar)arguments.get(0);
                    if(unresolvedStar.getTarget()==null){
                        arguments = ParserUtils.Seq(Literal.build(new Integer(1)));
                    }
                }


                FunctionIdentifier funcId = replaceFunctions(visitFunctionName(fctx.qualifiedName()), fctx);
                UnresolvedFunction function = new UnresolvedFunction(funcId, arguments, isDistinct);

                if(fctx.windowSpec() instanceof SqlBaseParser.WindowRefContext){
                    return new UnresolvedWindowExpression(function, (WindowSpecReference)visitWindowRef((SqlBaseParser.WindowRefContext)fctx.windowSpec()));
                }else if(fctx.windowSpec() instanceof SqlBaseParser.WindowDefContext){
                    return new WindowExpression(function, (WindowSpecDefinition)visitWindowDef((SqlBaseParser.WindowDefContext)fctx.windowSpec()));
                }else{
                    return function;
                }
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



    @Override
    public Expression visitLambda(SqlBaseParser.LambdaContext ctx){
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.LambdaContext, Expression>() {
            @Override
            public Expression apply(SqlBaseParser.LambdaContext lctx){
                List<NamedExpression> arguments = ParserUtils.map(lctx.IDENTIFIER(), new Function<TerminalNode, NamedExpression>() {
                    @Override
                    public NamedExpression apply(TerminalNode name){
                        return new UnresolvedNamedLambdaVariable(UnresolvedAttribute.quoted(name.getText()).nameParts);
                    }
                });

                //TODO:

                Expression e =expression(lctx.expression());
                Expression function =e;
                if(e instanceof UnresolvedAttribute){
                    UnresolvedAttribute a = (UnresolvedAttribute)e;
                    function = new UnresolvedNamedLambdaVariable(a.nameParts);
                }
                return new LambdaFunction(function, arguments);
            }
        });
    }






    @Override
    public WindowSpecReference visitWindowRef(SqlBaseParser.WindowRefContext ctx){
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.WindowRefContext, WindowSpecReference>() {
            @Override
            public WindowSpecReference apply(SqlBaseParser.WindowRefContext wctx){
                return new WindowSpecReference(wctx.identifier().getText());
            }
        });
    }


    @Override
    public WindowSpecDefinition visitWindowDef(SqlBaseParser.WindowDefContext ctx){
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.WindowDefContext, WindowSpecDefinition>() {
            @Override
            public WindowSpecDefinition apply(SqlBaseParser.WindowDefContext wctx) {
                List<Expression>partition =  ParserUtils.map(wctx.partition, new Function<SqlBaseParser.ExpressionContext, Expression>() {
                    @Override
                    public Expression apply(SqlBaseParser.ExpressionContext expressionContext) {
                        return expression(expressionContext);
                    }
                });

                List<SortOrder>order =  ParserUtils.map(wctx.sortItem(), new Function<SqlBaseParser.SortItemContext, SortOrder>() {
                    @Override
                    public SortOrder apply(SqlBaseParser.SortItemContext sortItemContext) {
                        return visitSortItem(sortItemContext);
                    }
                });

                SpecifiedWindowFrame frameSpecOption = null;
                FrameType frameType = null;
                SqlBaseParser.WindowFrameContext frame = wctx.windowFrame();
                switch (frame.frameType.getType()){
                    case SqlBaseParser.RANGE:
                        frameType = new RangeFrame();
                        break;
                    case SqlBaseParser.ROWS :
                        frameType= new RowFrame();
                        break;
                }

                //TODO:

                Expression end = new CurrentRow();
                if(frame.end!=null){
                    end = visitFrameBound(frame.end);
                }
                frameSpecOption = new SpecifiedWindowFrame(
                        frameType,
                        visitFrameBound(frame.start),
                        end);
                WindowFrame windowFrame = frameSpecOption;
                if(frameSpecOption==null){
                    windowFrame = new UnspecifiedFrame();
                }
                return new WindowSpecDefinition(
                        partition,
                        order,
                        windowFrame);
            }
        });
    }

    @Override
    public Expression visitFrameBound(SqlBaseParser.FrameBoundContext ctx) {
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.FrameBoundContext, Expression>() {
            @Override
            public Expression apply(SqlBaseParser.FrameBoundContext fctx) {

                Expression value = expression(fctx.expression());
                ParserUtils.validate(value.resolved() && value.isFoldable(), "Frame bound value must be a literal.", fctx);

                switch (fctx.boundType.getType()) {
                    case SqlBaseParser.PRECEDING:
                        if (fctx.UNBOUNDED() != null) {
                            return new UnboundedPreceding();
                        } else {
                            return new UnaryMinus(value);
                        }
                    case SqlBaseParser.CURRENT:
                        return new CurrentRow();
                    case SqlBaseParser.FOLLOWING:
                        if (fctx.UNBOUNDED() != null) {
                            return new UnboundedFollowing();
                        } else {
                            return value;
                        }
                }

                return null;
            }
        });
    }


        @Override
        public Expression visitRowConstructor(SqlBaseParser.RowConstructorContext ctx){
            return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.RowConstructorContext, Expression>() {
                @Override
                public Expression apply(SqlBaseParser.RowConstructorContext fctx) {

                    return CreateStruct.build(ParserUtils.map(fctx.namedExpression(), new Function<SqlBaseParser.NamedExpressionContext, Expression>() {
                        @Override
                        public Expression apply(SqlBaseParser.NamedExpressionContext c) {
                            return expression(c);
                        }
                    }));
                }
            });
        }


    @Override
    public Expression visitSubqueryExpression(SqlBaseParser.SubqueryExpressionContext ctx){
            return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.SubqueryExpressionContext, Expression>() {
                @Override
                public Expression apply(SqlBaseParser.SubqueryExpressionContext sctx) {
                    return new ScalarSubquery(plan(sctx.query()));
                }
            });
    }

    @Override
    public Expression visitSimpleCase(SqlBaseParser.SimpleCaseContext ctx){
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.SimpleCaseContext, Expression>() {
            @Override
            public Expression apply(SqlBaseParser.SimpleCaseContext sctx) {
                Expression e = expression(sctx.value);
                List<Pair<Expression,Expression>> branches = ParserUtils.map(sctx.whenClause(), new Function<SqlBaseParser.WhenClauseContext, Pair<Expression,Expression>>() {
                    @Override
                    public Pair<Expression,Expression> apply(SqlBaseParser.WhenClauseContext wctx){
                        return new Pair<>(new EqualTo(e, expression(wctx.condition)), expression(wctx.result));
                    }
                });
                return new CaseWhen(branches, expression(sctx.elseExpression));
            }
        });
    }

    @Override
    public Expression visitSearchedCase(SqlBaseParser.SearchedCaseContext ctx){
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.SearchedCaseContext, Expression>() {
            @Override
            public Expression apply(SqlBaseParser.SearchedCaseContext sctx) {
                List<Pair<Expression,Expression>> branches = ParserUtils.map(sctx.whenClause(), new Function<SqlBaseParser.WhenClauseContext, Pair<Expression,Expression>>() {
                    @Override
                    public Pair<Expression,Expression> apply(SqlBaseParser.WhenClauseContext wctx){
                        return new Pair<>(expression(wctx.condition), expression(wctx.result));
                    }
                });
                return new CaseWhen(branches, expression(sctx.elseExpression));
            }
        });
    }



    private boolean canApplyRegex(ParserRuleContext ctx){
        return ParserUtils.withOrigin(ctx, new Function<ParserRuleContext, Boolean>() {
            @Override
            public Boolean apply(ParserRuleContext pctx) {
                ParserRuleContext parent = pctx.getParent();
                while (parent != null) {
                    if (parent instanceof SqlBaseParser.NamedExpressionContext)
                        return true;
                    parent = parent.getParent();
                }
                return false;
            }
        });
    }


    @Override
    public Expression visitDereference(SqlBaseParser.DereferenceContext ctx){
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.DereferenceContext, Expression>() {
            @Override
            public Expression apply(SqlBaseParser.DereferenceContext dctx) {
                String attr = dctx.fieldName.getText();
                Expression e = expression(dctx.base);

//                match {
//                    case unresolved_attr @ UnresolvedAttribute(nameParts) =>
//                        ctx.fieldName.getStart.getText match {
//                        case escapedIdentifier(columnNameRegex)
//                            if conf.supportQuotedRegexColumnName && canApplyRegex(ctx) =>
//                            UnresolvedRegex(columnNameRegex, Some(unresolved_attr.name),
//                                    conf.caseSensitiveAnalysis)
//                        case _ =>
//                            UnresolvedAttribute(nameParts :+ attr)
//                    }
//                    case e =>
//                        UnresolvedExtractValue(e, Literal(attr))
//                }
                return null;
            }
        });
    }

    @Override
    public Expression visitColumnReference(SqlBaseParser.ColumnReferenceContext ctx){
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.ColumnReferenceContext, Expression>() {
            @Override
            public Expression apply(SqlBaseParser.ColumnReferenceContext columnReferenceContext) {
//                ctx.getStart.getText match {
//                    case escapedIdentifier(columnNameRegex)
//                        if conf.supportQuotedRegexColumnName && canApplyRegex(ctx) =>
//                        UnresolvedRegex(columnNameRegex, None, conf.caseSensitiveAnalysis)
//                    case _ =>
//                        UnresolvedAttribute.quoted(ctx.getText)
//                }
                return null;
            }
        });


    }



    @Override
    public Expression visitSubscript(SqlBaseParser.SubscriptContext ctx){
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.SubscriptContext, Expression>() {
            @Override
            public Expression apply(SqlBaseParser.SubscriptContext sctx) {
                return new UnresolvedExtractValue(expression(sctx.value), expression(sctx.index));
            }
        });
    }

    @Override
    public Expression visitParenthesizedExpression(SqlBaseParser.ParenthesizedExpressionContext ctx){
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.ParenthesizedExpressionContext, Expression>() {
            @Override
            public Expression apply(SqlBaseParser.ParenthesizedExpressionContext pctx) {
                return expression(pctx.expression());
            }
        });
    }

    @Override
    public SortOrder visitSortItem(SqlBaseParser.SortItemContext ctx) {
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.SortItemContext, SortOrder>() {
            @Override
            public SortOrder apply(SqlBaseParser.SortItemContext sortItemContext) {
                SortDirection direction;
                if (sortItemContext.DESC() != null) {
                    direction = new Descending();
                } else {
                    direction = new Ascending();
                }

                NullOrdering nullOrdering;
                if (sortItemContext.FIRST() != null) {
                    nullOrdering = new NullsFirst();
                } else if (sortItemContext.LAST() != null) {
                    nullOrdering = new NullsLast();
                } else {
                    nullOrdering = direction.getDefaultNullOrdering();
                }

                return new SortOrder(expression(sortItemContext.expression()), direction, nullOrdering);
            }
        });
    }


    @Override
    public Literal visitTypeConstructor(SqlBaseParser.TypeConstructorContext ctx){
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.TypeConstructorContext, Literal>() {
            @Override
            public Literal apply(SqlBaseParser.TypeConstructorContext tctx) {
                String value = ParserUtils.string(tctx.STRING());
                String valueType = tctx.identifier().getText().toUpperCase(Locale.ROOT);

                try{
                    switch(valueType){
                        case "DATE":
                            return Literal.build(java.sql.Date.valueOf(value));
                        case "TIMESTAMP":
                            return Literal.build(Timestamp.valueOf(value));
                        case "X":
                            String padding ="";
                            if (value.length() % 2 != 0) {
                                padding = "0";
                            }
                            return Literal.build(DatatypeConverter.parseHexBinary(padding + value));
                        default:
                            throw new ParseException("Literals of type '$other' are currently not supported.", tctx);
                    }
                }catch (IllegalArgumentException e){
                    String  message = e.getMessage();//Option(e.getMessage).getOrElse(s"Exception parsing $valueType")
                    throw new ParseException(message, tctx);
                }
            }
        });
    }


    @Override
    public Literal visitNullLiteral(SqlBaseParser.NullLiteralContext ctx){
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.NullLiteralContext, Literal>() {
            @Override
            public Literal apply(SqlBaseParser.NullLiteralContext nctx) {
                return Literal.build(null);
            }
        });
    }


    @Override
    public Literal visitBooleanLiteral(SqlBaseParser.BooleanLiteralContext ctx){
        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.BooleanLiteralContext, Literal>() {
            @Override
            public Literal apply(SqlBaseParser.BooleanLiteralContext bctx) {
                if (Boolean.valueOf(bctx.getText())) {
                    return Literal.TrueLiteral();
                } else {
                    return Literal.FalseLiteral();
                }
            }
        });

    }











































































































































































































































































































































































































//    @Override
//    public List<String> visitIdentifierList(SqlBaseParser.IdentifierListContext ctx) {
//        return ParserUtils.withOrigin(ctx, new Function<SqlBaseParser.IdentifierListContext, List<String>>() {
//            @Override
//            public List<String> apply(SqlBaseParser.IdentifierListContext identifierListContext) {
//                return visitIdentifierSeq(identifierListContext.identifierSeq());
//            }
//        });
//
//    }

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




    private String createString(SqlBaseParser.StringLiteralContext ctx){
//            if (conf.escapedStringLiterals) {
//                ctx.STRING().asScala.map(stringWithoutUnescape).mkString
//        } else {
//        ctx.STRING().asScala.map(string).mkString
//        }
//        }
        return null;
    }




}
