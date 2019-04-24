package org.apache.spark.sql.catalyst.plans.logical;

import org.apache.spark.lang.PartialFunction;
import org.apache.spark.sql.catalyst.plans.QueryPlan;

import java.util.function.Function;

/**
 * Created by kenya on 2019/1/18.
 */
public abstract class LogicalPlan extends QueryPlan<LogicalPlan> {
    //isStreaming: Boolean = children.exists(_.isStreaming == true)

    private boolean _analyzed = false;

    protected  boolean analyzed(){
        return _analyzed;
    }

    private ThreadLocal<Integer> resolveOperatorDepth = new ThreadLocal() {
        @Override
        public Integer initialValue() {
            return 0;
        }
    };

    public <T> T allowInvokingTransformsInAnalyzer(Function<Void, T> f) {
        resolveOperatorDepth.set(resolveOperatorDepth.get() + 1);
        try {
            return f.apply((Void) null);
        } finally {
            resolveOperatorDepth.set(resolveOperatorDepth.get() - 1);
        }
    }


    public LogicalPlan resolveOperatorsUp(PartialFunction<LogicalPlan, LogicalPlan> rule){
        if (!analyzed()) {
            return allowInvokingTransformsInAnalyzer(
                    (Void)->{
                        LogicalPlan afterRuleOnChildren = mapChildren((p)->{return p.resolveOperatorsUp(rule);});
                        if (this.fastEquals(afterRuleOnChildren)) {
                            Object o = CurrentOrigin.withOrigin( CurrentOrigin.get(),(V)->{
                                return rule.applyOrElse(this, (p)->{return p;});
                            });
                            return (LogicalPlan)o;
                        } else {
                            Object o = CurrentOrigin.withOrigin( CurrentOrigin.get(),(V)->{
                                return rule.applyOrElse(afterRuleOnChildren, (q)->{return q;});
                            });
                            return (LogicalPlan)o;
                        }
                    }
            );
        } else {
            return this;
        }
    }

    protected LogicalPlan resolveOperatorsDown(PartialFunction<LogicalPlan, LogicalPlan> rule) {
        if (!analyzed()) {
            return allowInvokingTransformsInAnalyzer(
                    (Void) -> {
                        LogicalPlan afterRule = CurrentOrigin.withOrigin(CurrentOrigin.get(), (V) -> {
                            return rule.applyOrElse(this,
                                    (p) -> {
                                        return this;
                                    });
                        });

                        if (this.fastEquals(afterRule)) {
                            return mapChildren((p) -> {
                                return p.resolveOperatorsDown(rule);
                            });
                        } else {
                            return afterRule.mapChildren((p) -> {
                                return p.resolveOperatorsDown(rule);
                            });
                        }
                    }
            );
        } else {
            return this;
        }
    }


    public LogicalPlan transformUp(PartialFunction<LogicalPlan, LogicalPlan> rule) {

        LogicalPlan afterRuleOnChildren = mapChildren((q) -> {
            return q.transformUp(rule);
        });
        if (fastEquals(afterRuleOnChildren)) {
            return CurrentOrigin.withOrigin(CurrentOrigin.get(), (Void) -> {
                return rule.applyOrElse(this, (p) -> {
                    return this;
                });
            });
        } else {
            return CurrentOrigin.withOrigin(CurrentOrigin.get(), (Void) -> {
                return rule.applyOrElse(afterRuleOnChildren, (p) -> {
                    return afterRuleOnChildren;
                });
            });
        }
    }


    public LogicalPlan transformDown(PartialFunction<LogicalPlan, LogicalPlan> rule) {

        LogicalPlan afterRule = CurrentOrigin.withOrigin(CurrentOrigin.get(), (c) -> {
            return rule.applyOrElse(this, (q) -> {
                return this;
            });
        });

        // Check if unchanged and then possibly return old copy to avoid gc churn.
        if (fastEquals(afterRule)) {
            return mapChildren((p) -> {
                return p.transformDown(rule);
            });
        } else {
            return afterRule.mapChildren((p) -> {
                return p.transformDown(rule);
            });
        }
    }

    public LogicalPlan transform(PartialFunction<LogicalPlan, LogicalPlan> rule) {
        return transformDown(rule);
    }

    @Override
    public String toString(){

        return this.getClass().getSimpleName();
    }
}
