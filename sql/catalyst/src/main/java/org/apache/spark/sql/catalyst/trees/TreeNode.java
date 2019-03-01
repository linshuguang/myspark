package org.apache.spark.sql.catalyst.trees;

import javafx.util.Pair;

import java.util.ArrayList;
import java.util.HashSet;
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

    private Set<TreeNode>containsChild;

    private Set<TreeNode> getContainsChild(){
        return new HashSet<>(children);
    }

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

    }

    public BaseType transformDown(Function<BaseType, BaseType> rule){
//        val afterRule = CurrentOrigin.withOrigin(origin) {
//            rule.applyOrElse(this, identity[BaseType])
//        }
//
//        // Check if unchanged and then possibly return old copy to avoid gc churn.
//        if (this fastEquals afterRule) {
//            mapChildren(_.transformDown(rule))
//        } else {
//            afterRule.mapChildren(_.transformDown(rule))
//        }
        return null;
    }

    public BaseType transformUp(Function<BaseType, BaseType> rule){

//        val afterRuleOnChildren = mapChildren(_.transformUp(rule))
//        if (this fastEquals afterRuleOnChildren) {
//            CurrentOrigin.withOrigin(origin) {
//                rule.applyOrElse(this, identity[BaseType])
//            }
//        } else {
//            CurrentOrigin.withOrigin(origin) {
//                rule.applyOrElse(afterRuleOnChildren, identity[BaseType])
//            }
//        }
        return (BaseType)this;
    }

    public boolean fastEquals(TreeNode other){
        return this.equals(other) || this == other;
    }

    public BaseType mapChildren(Function<BaseType,BaseType>f){
        if (children.size()>0) {
            boolean changed = false;

            Function<Object,Object> mapChild= new Function<Object, Object>() {
                @Override
                public Object apply(Object child){
                    if(child instanceof TreeNode){
                        if(getContainsChild().contains(child)){
                            TreeNode arg = (TreeNode)child;
                            BaseType newChild = f.apply((BaseType)arg);
                            if(newChild.fastEquals(arg)){
                                //TODO
                                //changed = true;
                                return newChild;
                            }else{
                                return arg;
                            }
                        }
                    }else if(child instanceof Pair){
                        Pair<TreeNode,TreeNode> args = (Pair<TreeNode,TreeNode>)child;
                        if(args!=null){
                            BaseType newChild1;
                            if(getContainsChild().contains(args.getKey())){
                                newChild1 = f.apply((BaseType) args.getKey());
                            }else{
                                newChild1 = (BaseType)args.getKey();
                            }
                            BaseType newChild2;
                            if(getContainsChild().contains(args.getValue())){
                                newChild2 = f.apply((BaseType) args.getValue());
                            }else{
                                newChild2 = (BaseType)args.getValue();
                            }

                            if (!(newChild1.fastEquals(args.getKey())) || !(newChild2.fastEquals(args.getValue()))) {
                                //TODO
                                //changed = true;
                                return new Pair<>(newChild1, newChild2);
                            } else {
                                return child;
                            }
                        }
                    }else{
                        return child;
                    }
                    return null;
                }
            };
        } else {
           return (BaseType)this;
        }
        return null;
    }
}
