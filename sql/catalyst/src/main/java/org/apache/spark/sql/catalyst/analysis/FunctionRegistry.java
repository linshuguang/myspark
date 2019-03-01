package org.apache.spark.sql.catalyst.analysis;

import javafx.util.Pair;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo;
import org.apache.spark.sql.catalyst.expressions.RuntimeReplaceable;
import org.apache.spark.sql.catalyst.expressions.arithmetic.Abs;
import org.apache.spark.sql.catalyst.identifiers.FunctionIdentifier;
import org.apache.spark.sql.catalyst.parser.ParserUtils;

import java.lang.reflect.Constructor;
import java.util.*;
import java.util.function.Function;

/**
 * Created by kenya on 2019/2/26.
 */
public abstract class FunctionRegistry {
    public abstract void registerFunction(
            FunctionIdentifier name,
            ExpressionInfo info,
            FunctionBuilder builder);

    private static Map<String,Pair<ExpressionInfo, FunctionBuilder>> expressions;

    static{
        expressions = new HashMap<>();
        Pair<String,Pair<ExpressionInfo, FunctionBuilder>> pair;
        pair = expression("abs", Abs.class);
        expressions.put(pair.getKey(),pair.getValue());

    }



    public static SimpleFunctionRegistry builtin(){
        SimpleFunctionRegistry fr = new SimpleFunctionRegistry();
        for(Map.Entry<String, Pair<ExpressionInfo, FunctionBuilder>> each:expressions.entrySet()){
            String name = each.getKey();
            ExpressionInfo info = each.getValue().getKey();
            FunctionBuilder builder = each.getValue().getValue();
            fr.registerFunction(new FunctionIdentifier(name), info, builder);
        }
        return fr;
    }


    private static <T extends Expression> Pair<String, Pair<ExpressionInfo, FunctionBuilder>> expression(String name,
            Class<T> tag){
        List<Constructor<?>> constructors;
        if(RuntimeReplaceable.class.isAssignableFrom(tag)){
            Constructor<?>[] all = tag.getConstructors();
            int maxNumArgs = Collections.max(ParserUtils.map(Arrays.asList(all),(q)->{ return q.getParameterCount();}));
            constructors = new ArrayList<>();
            for(Constructor<?> c: all){
                if(c.getParameterCount()==maxNumArgs){
                    constructors.add(c);
                }
            }
        }else {
            constructors = Arrays.asList(tag.getConstructors());
        }

        // See if we can find a constructor that accepts Seq[Expression]
        List<Constructor<?>> varargCtor = new ArrayList<>();
        for(Constructor<?> c: constructors){
            List<Class<?>> thisConstructor = Arrays.asList(c.getParameterTypes());
            boolean found = true;
            for(Class<?>cc: thisConstructor){
                if(!cc.isAssignableFrom(Expression.class)){
                    found = false;
                    break;
                }
            }
            if(found){
                varargCtor.add(c);
            }
        }

        FunctionBuilder builder = (expressions)->{ if(varargCtor.size()==1){
            try{
                Expression  e = (Expression)varargCtor.get(0).newInstance(expressions);
                return e;

            }catch (Exception e){
                throw new AnalysisException(e.getCause().getMessage());
            }
        }else{
            //TODO
        }
            return null;};



        return new Pair<String, Pair<ExpressionInfo, FunctionBuilder>>(name, new Pair<ExpressionInfo, FunctionBuilder>(new ExpressionInfo(tag.getCanonicalName(),name),builder));
    }
}
