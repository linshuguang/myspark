package org.apache.spark.sql.catalyst.analysis;

import javafx.util.Pair;
import org.apache.spark.sql.catalyst.catalog.InMemoryCatalog;
import org.apache.spark.sql.catalyst.catalog.SessionCatalog;
import org.apache.spark.sql.internal.SQLConf;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;

/**
 * Created by kenya on 2019/3/7.
 */
@Service
public class SimpleAnalyzer extends Analyzer {
    public SimpleAnalyzer(){
        super(new SessionCatalog(
                        new InMemoryCatalog(),
                        new EmptyFunctionRegistry(),
                new SQLConf().copy(new Pair<>(SQLConf.CASE_SENSITIVE , true))),
                new SQLConf().copy(new Pair<>(SQLConf.CASE_SENSITIVE , true))
                );
    }




//    Batch("Hints", fixedPoint,
//      new ResolveHints.ResolveBroadcastHints(conf),
//      ResolveHints.ResolveCoalesceHints,
//      ResolveHints.RemoveAllHints),
//    Batch("Simple Sanity Check", Once,
//      LookupFunctions),
//    Batch("Substitution", fixedPoint,
//      CTESubstitution,
//      WindowsSubstitution,
//      EliminateUnions,
//      new SubstituteUnresolvedOrdinals(conf)),
//    Batch("Resolution", fixedPoint,
//      ResolveTableValuedFunctions ::
//      ResolveRelations ::
//      ResolveReferences ::
//      ResolveCreateNamedStruct ::
//      ResolveDeserializer ::
//      ResolveNewInstance ::
//      ResolveUpCast ::
//      ResolveGroupingAnalytics ::
//      ResolvePivot ::
//      ResolveOrdinalInOrderByAndGroupBy ::
//      ResolveAggAliasInGroupBy ::
//      ResolveMissingReferences ::
//      ExtractGenerator ::
//      ResolveGenerate ::
//      ResolveFunctions ::
//      ResolveAliases ::
//      ResolveSubquery ::
//      ResolveSubqueryColumnAliases ::
//      ResolveWindowOrder ::
//      ResolveWindowFrame ::
//      ResolveNaturalAndUsingJoin ::
//      ResolveOutputRelation ::
//      ExtractWindowExpressions ::
//      GlobalAggregates ::
//      ResolveAggregateFunctions ::
//      TimeWindowing ::
//      ResolveInlineTables(conf) ::
//      ResolveHigherOrderFunctions(catalog) ::
//      ResolveLambdaVariables(conf) ::
//      ResolveTimeZone(conf) ::
//      ResolveRandomSeed ::
//      TypeCoercion.typeCoercionRules(conf) ++
//      extendedResolutionRules : _*),
//    Batch("Post-Hoc Resolution", Once, postHocResolutionRules: _*),
//    Batch("View", Once,
//      AliasViewChild(conf)),
//    Batch("Nondeterministic", Once,
//      PullOutNondeterministic),
//    Batch("UDF", Once,
//      HandleNullInputsForUDF),
//    Batch("FixNullability", Once,
//      FixNullability),
//    Batch("Subquery", Once,
//      UpdateOuterReferences),





}
