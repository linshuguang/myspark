package org.apache.spark.sql.catalyst.analysis.ResolveHints;

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.rules.Rule;
import org.apache.spark.sql.internal.SQLConf;

import java.util.function.BiFunction;

/**
 * Created by kenya on 2019/4/15.
 */
public class ResolveBroadcastHints extends Rule<LogicalPlan>{
    SQLConf conf;

    BiFunction<String,String, Boolean> resolver;
    public ResolveBroadcastHints(SQLConf conf){
        this.conf = conf;
        resolver = conf.resolver();
    }

    @Override
    public LogicalPlan apply(LogicalPlan plan){
        plan.transformUp()
        : LogicalPlan = plan resolveOperatorsUp
        case h: UnresolvedHint if BROADCAST_HINT_NAMES.contains(h.name.toUpperCase(Locale.ROOT)) =>
            if (h.parameters.isEmpty) {
                // If there is no table alias specified, turn the entire subtree into a BroadcastHint.
                ResolvedHint(h.child, HintInfo(broadcast = true))
            } else {
                // Otherwise, find within the subtree query plans that should be broadcasted.
                applyBroadcastHint(h.child, h.parameters.map {
                    case tableName: String => tableName
                    case tableId: UnresolvedAttribute => tableId.name
                    case unsupported => throw new AnalysisException("Broadcast hint parameter should be " +
                            s"an identifier or string but was $unsupported (${unsupported.getClass}")
                }.toSet)
            }
    }
}


}
