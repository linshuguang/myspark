package org.apache.spark.sql.catalyst.plans.logical.basicLogicalOperators;

import lombok.Data;
import org.apache.spark.sql.catalyst.parser.ParserUtils;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.UnaryNode;
import org.apache.spark.sql.catalyst.util.RandomSampler;

/**
 * Created by kenya on 2019/2/21.
 */
@Data
public class Sample extends UnaryNode {
    double lowerBound;
    double upperBound;
    boolean withReplacement;
    long seed;

    public Sample( Double lowerBound,
            double upperBound,
            boolean withReplacement,
                   long seed,
            LogicalPlan child){
        super(child);
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.withReplacement = withReplacement;
        this.seed = seed;

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

    @Override
    public Sample clone(){
        //TODO:deep copy check on child
        //throw new RuntimeException("todo here");
        return new Sample(lowerBound,upperBound,withReplacement,seed,getChild());
    }

    @Override
    public boolean equals(Object o){
        if(o instanceof Sample){
            Sample s = (Sample)o;
            return  Double.compare(lowerBound,s.lowerBound)==0
                    && Double.compare(upperBound,s.upperBound)==0
                    && withReplacement==s.withReplacement
                    && seed == s.seed;

        }
        return false;
    }
}
