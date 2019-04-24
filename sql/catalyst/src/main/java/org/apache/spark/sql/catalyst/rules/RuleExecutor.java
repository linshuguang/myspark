package org.apache.spark.sql.catalyst.rules;

import lombok.Data;
import org.apache.spark.internal.Logging;
import org.apache.spark.sql.catalyst.QueryPlanningTracker;
import org.apache.spark.sql.catalyst.errors.TreeNodeException;
import org.apache.spark.sql.catalyst.parser.ParserUtils;
import org.apache.spark.sql.catalyst.trees.TreeNode;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.util.Utils;

import javax.swing.text.ParagraphView;
import java.util.Arrays;
import java.util.List;

/**
 * Created by kenya on 2019/3/7.
 */

public abstract class RuleExecutor <TreeType extends TreeNode> {

    public static QueryExecutionMetering queryExecutionMeter = new QueryExecutionMetering();

    public String dumpTimeSpent(){
        return queryExecutionMeter.dumpTimeSpent();
    }

    /** Resets statistics about time spent running specific rules */
    public void resetMetrics(){
        queryExecutionMeter.resetMetrics();
    }




    public abstract class Strategy {
        int maxIterations;
        public Strategy(int maxIterations){this.maxIterations = maxIterations;}
    }

    public class Once extends Strategy {
        public Once(){
            super(1);
        }
    }

    public class FixedPoint extends Strategy{
        public FixedPoint(int maxIterations){
            super(maxIterations);
        }
    }



    public class Batch{
        String name;
        Strategy strategy;
        Rule[] rules;
        public Batch(String name,Strategy strategy, Rule...rules){
            this.name = name;
            this.strategy = strategy;
            this.rules = rules;
        }
    }


    protected Boolean isPlanIntegral(TreeType plan){
        return true;
    }

    public TreeType executeAndTrack(TreeType plan, QueryPlanningTracker tracker){
        QueryPlanningTracker  queryPlanningTracker = new QueryPlanningTracker();
        return queryPlanningTracker.withTracker(tracker,(Void)->{
            return execute(plan);
        });
    }

    protected abstract List<Batch> batches();


    public TreeType execute(TreeType plan){

        TreeType curPlan = plan;
        QueryExecutionMetering queryExecutionMetrics = RuleExecutor.queryExecutionMeter;
        PlanChangeLogger planChangeLogger = new PlanChangeLogger();
        QueryPlanningTracker tracker = QueryPlanningTracker.get();

        for(Batch batch:batches()){
            TreeType batchStartPlan = curPlan;
            int iteration = 1;
            TreeType lastPlan = curPlan;
            boolean ifContinue = true;
            while (ifContinue){
//                for(Rule rule : batch.rules){
//
//                }
                for(Rule rule:batch.rules){
                    long startTime = System.nanoTime();
                    TreeType result = (TreeType) rule.apply(curPlan);
                    long runTime = System.nanoTime() - startTime;
                    boolean effective = !result.fastEquals(curPlan);

                    if (effective) {
                        queryExecutionMetrics.incNumEffectiveExecution(rule.ruleName);
                        queryExecutionMetrics.incTimeEffectiveExecutionBy(rule.ruleName, runTime);
                        planChangeLogger.log(rule.ruleName, plan, result);
                    }

                    queryExecutionMetrics.incExecutionTimeBy(rule.ruleName, runTime);
                    queryExecutionMetrics.incNumExecution(rule.ruleName);
                    tracker.recordRuleInvocation(rule.ruleName, runTime, effective);
                    if (!isPlanIntegral(result)) {
                        String message = "After applying rule "+rule.ruleName+" in batch "+batch.name+", " +
                                "the structural integrity of the plan is broken.";
                        //TODO:
                        //throw new TreeNodeException(result, message, null);
                    }
                    curPlan = result;
                }

                iteration += 1;
                if (iteration > batch.strategy.maxIterations) {
                    // Only log if this is a rule that is supposed to run more than once.
                    if (iteration != 2) {
                        String message = "Max iterations ("+(iteration - 1)+") reached for batch ${batch.name}";
                        if (Utils.isTesting()) {
                            //TODO:
                            //throw new TreeNodeException(curPlan, message, null);
                        } else {
                            Logging.logWarning(message);
                        }
                    }
                    ifContinue = false;
                }

                if (curPlan.fastEquals(lastPlan)) {
                    logTrace(
                            "Fixed point reached for batch ${batch.name} after ${iteration - 1} iterations.");
                    ifContinue = false;
                }
                lastPlan = curPlan;
            }
            if (!batchStartPlan.fastEquals(curPlan)) {
                //TODO:
//                logDebug(
//                        s"""
//                                |=== Result of Batch ${batch.name} ===
//            |${sideBySide(batchStartPlan.treeString, curPlan.treeString).mkString("\n")}
//                """.stripMargin)
            } else {
                //TODO:
                //logTrace(s"Batch ${batch.name} has no effect.")
            }
        }
        return curPlan;
    }


    private class PlanChangeLogger{
        private String logLevel = SQLConf.get().optimizerPlanChangeLogLevel();
        private List<String> logRules = Arrays.asList(SQLConf.get().optimizerPlanChangeRules());

        public void log(String ruleName, TreeType oldPlan, TreeType newPlan){
            if (logRules.size()==0 || logRules.get(0).contains(ruleName)) {
                String message =
                        "\n=== Applying Rule "+ruleName+" ==="
                        +"${sideBySide(oldPlan.treeString, newPlan.treeString)";
                switch (logLevel){
                    case "TRACE" :
                        logTrace(message);
                        break;
                    case "DEBUG":
                        //logDebug(message);
                        break;
                    case "INFO":
                        //logInfo(message);
                                break;
                    case "WARN" :
                        //logWarning(message)
                        break;
                    case "ERROR" :
                        //=> logError(message)
                        break;
                    default:
                        //logTrace(message)
                }
            }
        }
    }
    protected void logTrace(String msg) {
        //if (log.isTraceEnabled) log.trace(msg)
    }
}
