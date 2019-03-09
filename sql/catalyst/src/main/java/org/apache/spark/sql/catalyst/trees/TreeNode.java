package org.apache.spark.sql.catalyst.trees;

import javafx.util.Pair;
import org.apache.commons.lang3.ClassUtils;
import org.apache.spark.lang.MurmurHash3;
import org.apache.spark.lang.PartialFunction;
import org.apache.spark.sql.catalyst.parser.ParserUtils;
import org.apache.spark.sql.types.DataType;
import org.codehaus.jackson.map.Serializers;

import static org.apache.spark.sql.catalyst.parser.ParserUtils.MutableObject;
import static org.apache.spark.sql.catalyst.errors.Errors.*;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;


/**
 * Created by kenya on 2019/1/18.
 */
public abstract class TreeNode<BaseType extends TreeNode<BaseType>> {
    BaseType self;

    abstract  protected List<BaseType> children();
    //Set<BaseType> containsChild =
    Origin origin = CurrentOrigin.get();

    protected List<Object> otherCopyArgs = new ArrayList<>();

    public Origin origin(){
        return CurrentOrigin.get();
    }

    private Set<TreeNode>containsChild;

    private Set<TreeNode> getContainsChild(){
        return new HashSet<>(children());
    }

    public static class Origin{
        Integer line;
        Integer startPosition;

        public Origin(Integer line, Integer startPosition){
            this.line = line;
            this.startPosition = startPosition;
        }

        public Origin(){
            this.line=null;
            this.startPosition = null;
        }

        private boolean equal(Integer i, Integer j){
            if(i==null && j==null){
                return true;
            }else if((i==null && j!=null)||i!=null && j==null){
                return false;
            }else{
                return i.compareTo(j)==0;
            }
        }

        @Override
        public boolean equals(Object other){
            if(other!=null && other instanceof Origin){
                Origin o = (Origin) other;
                if(this==o){
                    return true;
                }
                return equal(o.line,this.line) && equal(o.startPosition,this.startPosition);
            }
            return false;
        }
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

        public static <A>  A withOrigin(Origin o,Function<Void,A>f){
            set(o);
            A ret = null;
            try{
                ret = f.apply((Void) null);
            } finally {
                reset();
            }
            return ret;
        }

    }






    public BaseType transformDown(PartialFunction<BaseType, BaseType> rule){

        BaseType afterRule = CurrentOrigin.withOrigin(origin,(c)->{
            return rule.applyOrElse((BaseType) this, (q)->{return q;});
        });

        // Check if unchanged and then possibly return old copy to avoid gc churn.
        if (fastEquals(afterRule)) {
            return mapChildren((p)->{return p.transformDown(rule);});
        } else {
            return afterRule.mapChildren((p)->{return p.transformDown(rule);});
        }
    }

    public BaseType transform(PartialFunction<BaseType, BaseType>rule){
        return transformDown(rule);
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

    public Set<TreeNode>containsChild(){
        return new HashSet<>(children());
    }

    //take a few efforts here
    public BaseType mapChildren(Function<BaseType,BaseType>f) {
        if (children().size() > 0) {

            MutableObject<Boolean> changed = new MutableObject<>(false);

            Function<Object, Object> mapChild = new Function<Object, Object>() {
                @Override
                public Object apply(Object child) {

                    if (child instanceof TreeNode) {
                        if (getContainsChild().contains(child)) {
                            TreeNode arg = (TreeNode) child;
                            BaseType newChild = f.apply((BaseType) arg);
                            if (newChild.fastEquals(arg)) {
                                changed.set(true);
                                return newChild;
                            } else {
                                return arg;
                            }
                        }
                    } else if (child instanceof Pair) {
                        Pair<TreeNode, TreeNode> args = (Pair<TreeNode, TreeNode>) child;
                        if (args != null) {
                            BaseType newChild1;
                            if (getContainsChild().contains(args.getKey())) {
                                newChild1 = f.apply((BaseType) args.getKey());
                            } else {
                                newChild1 = (BaseType) args.getKey();
                            }
                            BaseType newChild2;
                            if (getContainsChild().contains(args.getValue())) {
                                newChild2 = f.apply((BaseType) args.getValue());
                            } else {
                                newChild2 = (BaseType) args.getValue();
                            }

                            if (!(newChild1.fastEquals(args.getKey())) || !(newChild2.fastEquals(args.getValue()))) {
                                changed.set(true);
                                return new Pair<>(newChild1, newChild2);
                            } else {
                                return child;
                            }
                        }
                    } else {
                        return child;
                    }


                    return null;
                }
            };

            Object[] newArgs = mapProductIterator(
                    new Function<Object, Object>() {
                        @Override
                        public Object apply(Object arg) {
                            if (arg instanceof TreeNode) {
                                TreeNode treeNode = (TreeNode) arg;
                                Set<TreeNode> treeNodes = ((TreeNode) arg).containsChild();
                                if (treeNodes != null && treeNodes.size() > 0) {
                                    BaseType newChild = f.apply((BaseType) arg);
                                    if (!treeNode.fastEquals(newChild)) {
                                        changed.set(true);
                                        return newChild;
                                    } else {
                                        return arg;
                                    }
                                }
                            } else if (arg instanceof Map) {
                                //trick
                                Map<Object, Object> mt = new HashMap<>();
                                Map<Object, Object> m = (Map) arg;
                                for (Map.Entry<Object, Object> entry : m.entrySet()) {
                                    Object key = entry.getKey();
                                    Object val = entry.getValue();

                                    if (val instanceof TreeNode) {
                                        TreeNode treeNode = (TreeNode) val;
                                        if (treeNode.containsChild().size() > 0) {
                                            BaseType newChild = f.apply((BaseType) val);
                                            if (!treeNode.fastEquals(newChild)) {
                                                changed.set(true);
                                                val = newChild;
                                            }
                                        }
                                    }
                                    mt.put(key, val);
                                }
                                return mt;
                            } else if (arg instanceof DataType) {
                                return arg;
                            } else {
                                //TODO: stream and trversable
                            }
                            return arg;
                        }
                    });

            if(changed.get()){
                return makeCopy(newArgs);
            }else{
                return (BaseType) this;
            }
        } else {
            return (BaseType) this;
        }
    }

    public void foreachUp(Function<BaseType,Void>f){
        for(BaseType child:children()){
            f.apply(child);
        }
        f.apply((BaseType) this);
    }

    @Override
    public int hashCode(){
        //TODO make lazy
        return MurmurHash3.productHash(this);
    }

    //iterate over this object, and transform any fields, e.g. condition/children
    protected <B> B[] mapProductIterator(Function<Object,B> f){

        Field[] fields =this.getClass().getDeclaredFields();
        List<B> arr = new ArrayList<>();
        for(int i=0; i<fields.length;i++) {
            try {
                boolean origin = fields[i].isAccessible();
                fields[i].setAccessible(true);
                arr.add(f.apply(fields[i].get(this)));
                fields[i].setAccessible(origin);
            }catch (IllegalAccessException e){
                continue;
            }
        }
        return (B[])arr.toArray();
    }




    public BaseType makeCopy(Object[] newArgs){

        Object[] _newArgs = newArgs;
        return attachTree(this, "makeCopy",(q)->{
            Stream<Constructor> stream = Arrays.stream(getClass().getConstructors());
            Constructor[] ctors = (Constructor[])stream.filter(line->line.getParameterTypes().length!=0).toArray();

            if(ctors.length==0){
                //TODO:sys.err
            }

            List<Object>allArgs = Arrays.asList(_newArgs);
            if (otherCopyArgs.size()>0) {
                allArgs.addAll(otherCopyArgs);
            }

            boolean found = false;
            Constructor defaultCtor=null;
            for(Constructor ctor:ctors){
                if(ctor.getParameterTypes().length==allArgs.size() && !allArgs.contains(null)){
                    List<Class> argsArray = ParserUtils.map(allArgs,(c)->{ return c.getClass();});
                    Class[] classes = new Class[argsArray.size()];
                    found = ClassUtils.isAssignable(classes,ctor.getParameterTypes(),true);
                    if(found){
                        defaultCtor = ctor;
                        break;
                    }
                }
            }
            if(!found){
                int max = 0;
                for(Constructor ctor:ctors){
                    if(ctor.getParameterTypes().length>max){
                        max = ctor.getParameterTypes().length;
                        defaultCtor = ctor;
                    }
                }
            }

            Constructor defaultCtor2 = defaultCtor;
            try {
                return CurrentOrigin.withOrigin(origin,(c)->{
                    try {
                        return (BaseType) defaultCtor2.newInstance(allArgs.toArray());
                    }catch (Exception e){
                        throw new IllegalArgumentException();
                    }

                });
            } catch(IllegalArgumentException e) {

            }catch (Exception e){

            }
            return null;
        });
    }

}
