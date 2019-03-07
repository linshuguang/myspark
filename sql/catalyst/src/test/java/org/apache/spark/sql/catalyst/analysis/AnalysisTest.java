package org.apache.spark.sql.catalyst.analysis;

import javafx.util.Pair;
import org.apache.spark.sql.catalyst.catalog.CatalogDatabase;
import org.apache.spark.sql.catalyst.catalog.InMemoryCatalog;
import org.apache.spark.sql.catalyst.catalog.SessionCatalog;
import org.apache.spark.sql.catalyst.plans.PlanTest;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.internal.SQLConf;

import java.net.URI;
import java.util.HashMap;
import java.util.List;

/**
 * Created by kenya on 2019/2/26.
 */
public class AnalysisTest extends PlanTest {

    protected Analyzer caseSensitiveAnalyzer = makeAnalyzer(true);
    protected Analyzer caseInsensitiveAnalyzer = makeAnalyzer(false);

    private Analyzer makeAnalyzer(boolean caseSensitive){
        SQLConf conf = new SQLConf().copy(new Pair<>(new SQLConf().CASE_SENSITIVE ,caseSensitive));
        SessionCatalog catalog = new SessionCatalog(new InMemoryCatalog(), FunctionRegistry.builtin(), conf);
        try {
            catalog.createDatabase(
                    new CatalogDatabase("default", "", new URI("loc"), new HashMap<>()),
                    false);
        }catch (Exception e){
            return null;
        }
        catalog.createTempView("TaBlE", TestRelations.testRelation,  true);
        catalog.createTempView("TaBlE2", TestRelations.testRelation2, true);
        catalog.createTempView("TaBlE3", TestRelations.testRelation3, true);
        Analyzer analyzer = new Analyzer(catalog, conf);
//        analyzer.setExtendedResolutionRules();
//            override val extendedResolutionRules = EliminateSubqueryAliases :: Nil
//        }
        return analyzer;
    }

    protected Analyzer getAnalyzer(boolean caseSensitive){
        if (caseSensitive)
            return caseSensitiveAnalyzer;
        else
            return caseInsensitiveAnalyzer;
    }

    protected void assertAnalysisError(
            LogicalPlan  inputPlan,
            List<String>  expectedErrors){
        assertAnalysisError(inputPlan,expectedErrors,true);
    }

    protected void assertAnalysisError(
            LogicalPlan  inputPlan,
            List<String>  expectedErrors,
            boolean caseSensitive){
        Analyzer analyzer = getAnalyzer(caseSensitive);
//        val e = intercept[AnalysisException] {
//            analyzer.checkAnalysis(analyzer.execute(inputPlan))
//        }
//
//        if (!expectedErrors.map(_.toLowerCase(Locale.ROOT)).forall(
//                e.getMessage.toLowerCase(Locale.ROOT).contains)) {
//            fail(
//                    s"""Exception message should contain the following substrings:
//                            |
//                    |  ${expectedErrors.mkString("\n  ")}
//           |
//           |Actual exception message:
//           |
//           |  ${e.getMessage}
//            """.stripMargin)
//        }
    }


//    private def makeAnalyzer(caseSensitive: Boolean): Analyzer = {
//        val conf = new SQLConf().copy(SQLConf.CASE_SENSITIVE -> caseSensitive)
//        val catalog = new SessionCatalog(new InMemoryCatalog, FunctionRegistry.builtin, conf)
//        catalog.createDatabase(
//                CatalogDatabase("default", "", new URI("loc"), Map.empty),
//                ignoreIfExists = false)
//        catalog.createTempView("TaBlE", TestRelations.testRelation, overrideIfExists = true)
//        catalog.createTempView("TaBlE2", TestRelations.testRelation2, overrideIfExists = true)
//        catalog.createTempView("TaBlE3", TestRelations.testRelation3, overrideIfExists = true)
//        new Analyzer(catalog, conf) {
//            override val extendedResolutionRules = EliminateSubqueryAliases :: Nil
//        }
//    }
}
