package org.apache.spark.sql.catalyst;

import com.sun.javafx.binding.StringFormatter;
import javafx.util.Pair;
import org.apache.spark.sql.catalyst.rules.Rule;
import org.apache.spark.sql.catalyst.util.BoundedPriorityQueue;

import java.util.*;
import java.util.function.Function;

/**
 * Created by kenya on 2019/4/8.
 */
public class QueryPlanningTracker {

    public class RuleSummary{
        long totalTimeNs;
        long numInvocations;
        long numEffectiveInvocations;
        public RuleSummary(long totalTimeNs,long numInvocations,long numEffectiveInvocations){
            this.totalTimeNs = totalTimeNs;
            this.numInvocations = numInvocations;
            this.numEffectiveInvocations = numEffectiveInvocations;
        }
        public RuleSummary(){
            this(0,0,0);

        }
        @Override
        public String toString(){

            return "RuleSummary("+totalTimeNs+", "+numInvocations+", "+numEffectiveInvocations+")";
        }
    }

    public class PhaseSummary{
        long startTimeMs;
        long endTimeMs;

        public PhaseSummary(long startTimeMs, long endTimeMs){
            this.startTimeMs = startTimeMs;
            this.endTimeMs = endTimeMs;
        }

        public long durationMs(){
            return endTimeMs - startTimeMs;
        }

        @Override
        public String toString(){
                return "PhaseSummary("+startTimeMs+", "+endTimeMs+")";
        }
    }

    private Map<String,RuleSummary> rulesMap = new HashMap<>();

    // From a phase to its start time and end time, in ms.
    private Map<String,PhaseSummary> phasesMap = new HashMap<>();

    /**
     * Measure the start and end time of a phase. Note that if this function is called multiple
     * times for the same phase, the recorded start time will be the start time of the first call,
     * and the recorded end time will be the end time of the last call.
     */
    public <T> T measurePhase(String phase,Function<Void,T>f){
        long startTime = System.currentTimeMillis();
        T ret = f.apply((Void)null);
        long endTime = System.currentTimeMillis();

        if (phasesMap.containsKey(phase)) {
            PhaseSummary oldSummary = phasesMap.get(phase);
            phasesMap.put(phase, new PhaseSummary(oldSummary.startTimeMs, endTime));
        } else {
            phasesMap.put(phase, new PhaseSummary(startTime, endTime));
        }
        return ret;
    }

    /**
     * Record a specific invocation of a rule.
     *
     * @param rule name of the rule
     * @param timeNs time taken to run this invocation
     * @param effective whether the invocation has resulted in a plan change
     */
    public void recordRuleInvocation(String rule, Long timeNs, Boolean effective){
        RuleSummary s = rulesMap.get(rule);
        if (s==null) {
            s = new RuleSummary();
            rulesMap.put(rule, s);
        }

        s.totalTimeNs += timeNs;
        s.numInvocations += 1;
        if(effective){
            s.numEffectiveInvocations +=1;
        }
    }

    // ------------ reporting functions below ------------

    public Map<String,RuleSummary> rules() {
        return rulesMap;
    }

    public Map<String,PhaseSummary> phases(){
        return phasesMap;
    }

    /**
     * Returns the top k most expensive rules (as measured by time). If k is larger than the rules
     * seen so far, return all the rules. If there is no rule seen so far or k <= 0, return empty seq.
     */
    public List<Pair<String,RuleSummary>> topRulesByTime(int k){
        if (k <= 0) {
            return new ArrayList<>();
        } else {
            Comparator<RuleSummary>  orderingByTime = new Comparator<RuleSummary>() {
                @Override
                public int compare(RuleSummary o1, RuleSummary o2) {
                    if(o1.totalTimeNs>o2.totalTimeNs){
                        return 1;
                    }else if(o1.totalTimeNs<o2.totalTimeNs){
                        return -1;
                    }else{
                        return 0;
                    }
                }
            };
            BoundedPriorityQueue<RuleSummary> q = new BoundedPriorityQueue<>(k,orderingByTime);
            for(Map.Entry<String,RuleSummary> entry:rulesMap.entrySet()){
                q.assign(entry.getValue());
            }
            return null;

//            for(RuleSummary ruleSummary: q.iterator()){
//                iterator
//            }
//            q.toSeq.sortBy(r => -r._2.totalTimeNs)
        }
    }


    private static ThreadLocal<QueryPlanningTracker> localTracker = new ThreadLocal() {
        @Override
        public QueryPlanningTracker initialValue(){
            return null;
        }
    };

    public static QueryPlanningTracker  get(){
        return (localTracker.get());
    }

    public <T> T withTracker(QueryPlanningTracker tracker,Function<Void,T>f){
        QueryPlanningTracker originalTracker = localTracker.get();
        localTracker.set(tracker);
        try {
            return f.apply((Void)null);
        }finally {
            localTracker.set(originalTracker);
        }
    }



}
