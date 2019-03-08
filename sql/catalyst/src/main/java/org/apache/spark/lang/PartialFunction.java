package org.apache.spark.lang;

import java.util.function.Function;

/**
 * Created by kenya on 2019/3/7.
 */
public  class PartialFunction<A,B> {

    Function<A,B> f;
    Function<A,Boolean>isDefinedAt;

    public PartialFunction(Function<A,Boolean>isDefinedAt,Function<A,B>f){
        this.isDefinedAt = isDefinedAt;
        this.f = f;
    }

    public B apply(A x){
        return f.apply(x);
    }

    public B applyOrElse(A x,Function<A,B>def){
        if(!isDefinedAt(x)){
            return def.apply(x);
        }else{
            return f.apply(x);
        }
    }

    public boolean isDefinedAt(A x){
        return isDefinedAt.apply(x);
    }

}
