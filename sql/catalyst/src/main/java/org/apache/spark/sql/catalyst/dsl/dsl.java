package org.apache.spark.sql.catalyst.dsl;

import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases;
import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer;
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute;
import org.apache.spark.sql.catalyst.analysis.unresolved.UnresolvedAlias;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.namedExpressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias;
import org.apache.spark.sql.catalyst.plans.logical.basicLogicalOperators.Project;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.util.BeanLoader;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by kenya on 2019/4/9.
 */
public class dsl {
    public static LogicalPlan analyze(LogicalPlan logicalPlan){
        SimpleAnalyzer simpleAnalyzer = (SimpleAnalyzer)BeanLoader.getBeans(SimpleAnalyzer.class);
        EliminateSubqueryAliases eliminateSubqueryAliases = new EliminateSubqueryAliases();
        return eliminateSubqueryAliases.apply(simpleAnalyzer.execute(logicalPlan));
    }


    public static AttributeReference applyInt(String s){
        return new AttributeReference(s, new IntegerType(),true);
    }

    public static LogicalPlan subquery(LogicalPlan logicalPlan, String alias){
      return new SubqueryAlias(alias, logicalPlan);
    }

    public static LogicalPlan select(LogicalPlan logicalPlan, Expression...exprs){
        List<NamedExpression>namedExpressions  = new ArrayList<>();
        for(Expression expr: exprs){
            if(expr instanceof NamedExpression){
                namedExpressions.add((NamedExpression) expr);
            }else{
                namedExpressions.add(new UnresolvedAlias(expr));
            }
        }
        return new Project(namedExpressions, logicalPlan);
    }

//    public static LogicalPlan analyze(LogicalPlan logicalPlan){
//        SimpleAnalyzer simpleAnalyzer = new SimpleAnalyzer();
//        EliminateSubqueryAliases eliminateSubqueryAliases = new EliminateSubqueryAliases();
//        return eliminateSubqueryAliases.apply(simpleAnalyzer.execute(logicalPlan));
//    }

    public static UnresolvedAttribute attr(String s){
        return new UnresolvedAttribute(s);
    }


}
