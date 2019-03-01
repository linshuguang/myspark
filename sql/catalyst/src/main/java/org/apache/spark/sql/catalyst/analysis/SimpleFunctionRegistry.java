package org.apache.spark.sql.catalyst.analysis;

import javafx.util.Pair;
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo;
import org.apache.spark.sql.catalyst.identifiers.FunctionIdentifier;
import javax.annotation.concurrent.GuardedBy;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Created by kenya on 2019/2/27.
 */
public class SimpleFunctionRegistry extends FunctionRegistry {


    @GuardedBy("this")
    private Map<FunctionIdentifier, Pair<ExpressionInfo, FunctionBuilder>> functionBuilders = new HashMap<>();



    private FunctionIdentifier normalizeFuncName(FunctionIdentifier name){
        return new FunctionIdentifier(name.getIdentifier().toLowerCase(Locale.ROOT), name.getDatabase());
    }

    @Override
    public void registerFunction(
            FunctionIdentifier name,
            ExpressionInfo info,
            FunctionBuilder builder){
        synchronized(this) {
            functionBuilders.put(normalizeFuncName(name), new Pair<>(info, builder));
        }
    }
}
