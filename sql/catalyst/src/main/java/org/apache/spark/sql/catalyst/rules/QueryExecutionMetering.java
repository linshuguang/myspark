package org.apache.spark.sql.catalyst.rules;

import com.google.common.util.concurrent.AtomicLongMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.catalyst.parser.ParserUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by kenya on 2019/4/8.
 */
public class QueryExecutionMetering {

    private AtomicLongMap<String> timeMap = AtomicLongMap.create();
    private AtomicLongMap<String> numRunsMap = AtomicLongMap.create();
    private AtomicLongMap<String> numEffectiveRunsMap = AtomicLongMap.create();
    private AtomicLongMap<String> timeEffectiveRunsMap = AtomicLongMap.create();

    /** Resets statistics about time spent running specific rules */
    public void resetMetrics(){
        timeMap.clear();
        numRunsMap.clear();
        numEffectiveRunsMap.clear();
        timeEffectiveRunsMap.clear();
    }

    public long totalTime(){
        return timeMap.sum();
    }

    public long totalNumRuns(){
        return numRunsMap.sum();
    }

    public void incExecutionTimeBy(String ruleName, long delta){
        timeMap.addAndGet(ruleName, delta);
    }

    public void incTimeEffectiveExecutionBy(String ruleName, long delta){
        timeEffectiveRunsMap.addAndGet(ruleName, delta);
    }

    public void incNumEffectiveExecution(String ruleName){
        numEffectiveRunsMap.incrementAndGet(ruleName);
    }

    public void incNumExecution(String ruleName){
        numRunsMap.incrementAndGet(ruleName);
    }

    /** Dump statistics about time spent running specific rules. */
    public String dumpTimeSpent(){
        Map<String,Long> map = timeMap.asMap();
        int maxLengthRuleNames = 0;
        for(String s: map.keySet()){
            if(s.length()>maxLengthRuleNames){
                maxLengthRuleNames = s.length();
            }
        }

        String colRuleName = "Rule"+StringUtils.rightPad(" ",maxLengthRuleNames);
        String colRunTime = "Effective Time / Total Time"+StringUtils.rightPad(" ",47);
        String colNumRuns = "Effective Runs / Total Runs"+StringUtils.rightPad(" ",47);

        Map<Long,String> treeMap = new TreeMap<>();

        for(Map.Entry<String,Long> entry:map.entrySet()){
            treeMap.put(entry.getValue(),entry.getKey());
        }

        StringBuilder ruleMetrics = new StringBuilder();
        for(Map.Entry<Long,String> entry:treeMap.entrySet()){
            String name = entry.getValue();
            Long time = entry.getKey();
            Long timeEffectiveRun = timeEffectiveRunsMap.get(name);
            Long numRuns = numRunsMap.get(name);
            Long numEffectiveRun = numEffectiveRunsMap.get(name);

            String ruleName = StringUtils.leftPad(name,maxLengthRuleNames, " ");
            String runtimeValue = StringUtils.rightPad(String.valueOf(timeEffectiveRun / time),47, " ");
            String numRunValue = StringUtils.rightPad(String.valueOf(numEffectiveRun / numRuns),47, " ");
            ruleMetrics.append("\n");
            ruleMetrics.append(ruleName);
            ruleMetrics.append(runtimeValue);
            ruleMetrics.append(numRunValue);
            ruleMetrics.append("\n");
        }

        return "\n=== Metrics of Analyzer/Optimizer Rules ==="
                +"\nTotal number of runs: "+ totalNumRuns()
                +"\nTotal time: "+totalTime() / 1000000000D+" seconds"
                +"\n"+colRuleName+" "+colRunTime+" "+colNumRuns+"\n"
                +ruleMetrics;
    }
}
