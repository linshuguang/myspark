package org.apache.spark.sql.catalyst.analysis;

import lombok.Data;
import org.springframework.stereotype.Service;

import java.util.function.Function;

/**
 * Created by kenya on 2019/4/9.
 */
@Service
public class AnalysisContext {
    String defaultDatabase;
    int nestedViewDepth;

    public AnalysisContext(String defaultDatabase,int nestedViewDepth){
        this.defaultDatabase = defaultDatabase;
        this.nestedViewDepth = nestedViewDepth;
    }
    public AnalysisContext(){
        this(null,0);
    }

    private ThreadLocal<AnalysisContext> value = new ThreadLocal() {
        @Override
        public AnalysisContext initialValue(){
           return new AnalysisContext();
        }
    };

    public  AnalysisContext get(){
        return value.get();
    }

    public void reset(){
        value.remove();
    }

    private void set(AnalysisContext context){
        value.set(context);
    }

     public <A> A withAnalysisContext(String database, Function<Void,A>f){
         AnalysisContext originContext = value.get();
         AnalysisContext context = new AnalysisContext(database,
                originContext.nestedViewDepth + 1);
        set(context);
        try {
            return f.apply((Void)null);
        }finally { set(originContext); }
    }
}
