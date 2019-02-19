package org.apache.spark.sql.catalyst.plans.logical;

import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.Expression;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by kenya on 2019/2/14.
 */
public  class  ScriptTransformation extends UnaryNode {

    List<Expression> input;
    String script;
    List<? extends Attribute> output;
    LogicalPlan child;
    ScriptInputOutputSchema ioschema;

    public ScriptTransformation(
            List<Expression> input,
            String script,
            List<? extends Attribute> output,
            LogicalPlan child,
            ScriptInputOutputSchema ioschema){
        this.input = input;
        this.script = script;
        this.output = output;
        this.child = child;
        this.ioschema = ioschema;
    }

//    public ScriptTransformation(
//            List<Expression> input,
//            String script,
//            List<AttributeReference> output,
//            LogicalPlan child,
//            ScriptInputOutputSchema ioschema){
//        this(input, script,new ArrayList<Attribute>(),child, ioschema);
//    }
}
