package org.apache.spark.sql.catalyst.trees;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * Created by kenya on 2019/1/18.
 */
public class TreeNode<BaseType extends TreeNode<BaseType>> {
    BaseType self;
    protected List<BaseType> children = new ArrayList<BaseType>();

    //Set<BaseType> containsChild =

    public static class Origin{
        Integer line = null;
        Integer startPosition = null;

        public Origin(Integer line, Integer startPosition){
            this.line = line;
            this.startPosition = startPosition;
        }

        public Origin(){}
    }

    public static class CurrentOrigin{
        private static ThreadLocal<Origin> value = new ThreadLocal<Origin>() {
            @Override
            protected Origin initialValue() {
                return new Origin();
            }
        };

        public static Origin get(){
            return value.get();
        }

        public static void set(Origin o){
            value.set(o);
        }

        public static void reset(){
            value.set(new Origin());
        }

        public static void setPosition(Integer line, Integer start){
            //value.set(value.get.copy(line = Some(line), startPosition = Some(start)))
            value.set(new Origin(line, start));
        }

//        public static <FunctionBuilder>  FunctionBuilder withOrigin(Origin o, Function<Origin, FunctionBuilder> f){
//            set(o);
//            FunctionBuilder ret =null;
//            try{
//                ret = f.apply(o);
//            }finally {
//                reset();
//            }
//
//        }
//        def withOrigin[A](o: Origin)(f: => A): A = {
//            set(o)
//            val ret = try f finally { reset() }
//            ret
//        }







    }

}
