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
        return self;
    }


//    public BaseType mapChildren(Function<BaseType,BaseType>f){
//        if (children.size()>0) {
//            var changed = false
//            def mapChild(child: Any): Any = child match {
//                case arg: TreeNode[_] if containsChild(arg) =>
//                    val newChild = f(arg.asInstanceOf[BaseType])
//                    if (!(newChild fastEquals arg)) {
//                    changed = true
//                    newChild
//                } else {
//                    arg
//                }
//                case tuple@(arg1: TreeNode[_], arg2: TreeNode[_]) =>
//                    val newChild1 = if (containsChild(arg1)) {
//                    f(arg1.asInstanceOf[BaseType])
//                } else {
//                    arg1.asInstanceOf[BaseType]
//                }
//
//                    val newChild2 = if (containsChild(arg2)) {
//                    f(arg2.asInstanceOf[BaseType])
//                } else {
//                    arg2.asInstanceOf[BaseType]
//                }
//
//                    if (!(newChild1 fastEquals arg1) || !(newChild2 fastEquals arg2)) {
//                    changed = true
//                    (newChild1, newChild2)
//                } else {
//                    tuple
//                }
//                case other => other
//            }
//
//            val newArgs = mapProductIterator {
//                case arg: TreeNode[_] if containsChild(arg) =>
//                    val newChild = f(arg.asInstanceOf[BaseType])
//                    if (!(newChild fastEquals arg)) {
//                    changed = true
//                    newChild
//                } else {
//                    arg
//                }
//                case Some(arg: TreeNode[_]) if containsChild(arg) =>
//                    val newChild = f(arg.asInstanceOf[BaseType])
//                    if (!(newChild fastEquals arg)) {
//                    changed = true
//                    Some(newChild)
//                } else {
//                    Some(arg)
//                }
//                case m: Map[_, _] => m.mapValues {
//                    case arg: TreeNode[_] if containsChild(arg) =>
//                        val newChild = f(arg.asInstanceOf[BaseType])
//                        if (!(newChild fastEquals arg)) {
//                        changed = true
//                        newChild
//                    } else {
//                        arg
//                    }
//                    case other => other
//                }.view.force // `mapValues` is lazy and we need to force it to materialize
//                case d: DataType => d // Avoid unpacking Structs
//                case args: Stream[_] => args.map(mapChild).force // Force materialization on stream
//                case args: Traversable[_] => args.map(mapChild)
//                case nonChild: AnyRef => nonChild
//                case null => null
//            }
//            if (changed) makeCopy(newArgs) else this
//        } else {
//           return this;
//        }
//    }
}
