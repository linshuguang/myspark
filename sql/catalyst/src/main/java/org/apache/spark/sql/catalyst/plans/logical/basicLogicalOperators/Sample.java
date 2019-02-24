package org.apache.spark.sql.catalyst.plans.logical.basicLogicalOperators;

import org.apache.spark.sql.catalyst.parser.ParserUtils;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.UnaryNode;
import org.apache.spark.sql.catalyst.util.RandomSampler;

/**
 * Created by kenya on 2019/2/21.
 */
public class Sample extends UnaryNode {
    Double lowerBound;
    Double upperBound;
    boolean withReplacement;
    Long seed;
    LogicalPlan child;

    public Sample( Double lowerBound,
            Double upperBound,
            boolean withReplacement,
            Long seed,
            LogicalPlan child){
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.withReplacement = withReplacement;
        this.seed = seed;
        this.child = child;

        Double eps = RandomSampler.roundingEpsilon;
        Double fraction = upperBound - lowerBound;
        if (withReplacement) {
            ParserUtils.require(
                    fraction >= 0.0 - eps,
                    "Sampling fraction ($fraction) must be nonnegative with replacement");
        } else {
            ParserUtils.require(
                    fraction >= 0.0 - eps && fraction <= 1.0 + eps,
                    "Sampling fraction ($fraction) must be on interval [0, 1] without replacement");
        }
    }
}
