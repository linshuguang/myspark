package org.apache.spark.sql.catalyst.expressions;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.catalyst.parser.ParserUtils;
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

    public Generate(Generator generator,
                     List<Integer>unrequiredChildIndex,
                     boolean outer,
                     String qualifier,
                     List<Attribute> generatorOutput,
                     LogicalPlan child){
        super(child);
        this.generator = generator;
        this.unrequiredChildIndex = unrequiredChildIndex;
        this.outer = outer;
        this.qualifier = qualifier;
        this.generatorOutput = generatorOutput;

    }

    @Override
    public boolean equals(Object o){
        if(o instanceof Generate){
            Generate g = (Generate)o;

            if(!ParserUtils.equals(g.generator,generator)){
                return false;
            }
            if(!ParserUtils.equalList(unrequiredChildIndex,g.unrequiredChildIndex)){
                return false;
            }
            if(outer!=g.outer){
                return false;
            }
            if(!StringUtils.equals(qualifier,g.qualifier)){
                return false;
            }
            if(!ParserUtils.equalList(generatorOutput,g.generatorOutput)){
                return false;
            }
            return true;
        }
        return false;
    }
}
