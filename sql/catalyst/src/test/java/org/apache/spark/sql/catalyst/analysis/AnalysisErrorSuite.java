package org.apache.spark.sql.catalyst.analysis;

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.junit.runner.RunWith;
import org.junit.runner.Runner;

import java.util.List;

import static org.apache.spark.sql.catalyst.analysis.TestRelations.*;
/**
 * Created by kenya on 2019/2/26.
 */
@SuppressWarnings("ALL")
@RunWith(Runner.class)
public class AnalysisErrorSuite extends AnalysisTest {

    private void errorTest(
            String name,
            LogicalPlan plan,
            List<String> errorMessages,
            boolean caseSensitive){
        test(name,()-> {
            //assertAnalysisError(plan, errorMessages, caseSensitive)
        });
    }

}
