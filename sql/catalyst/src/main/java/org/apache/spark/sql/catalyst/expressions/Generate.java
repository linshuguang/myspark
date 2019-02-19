package org.apache.spark.sql.catalyst.expressions;

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.UnaryNode;

import java.util.List;

/**
 * Created by kenya on 2019/2/15.
 */
public class Generate extends UnaryNode{
    Generator generator;
    List<Integer> unrequiredChildIndex;
    boolean outer;
    String qualifier;
    List<Attribute> generatorOutput;
    LogicalPlan child;

    public Generate(Generator generator,
                     List<Integer>unrequiredChildIndex,
                     boolean outer,
                     String qualifier,
                     List<Attribute> generatorOutput,
                     LogicalPlan child){
        this.generator = generator;
        this.unrequiredChildIndex = unrequiredChildIndex;
        this.outer = outer;
        this.qualifier = qualifier;
        this.generatorOutput = generatorOutput;
        this.child = child;

    }
}
