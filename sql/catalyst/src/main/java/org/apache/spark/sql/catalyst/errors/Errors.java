package org.apache.spark.sql.catalyst.errors;

import lombok.experimental.NonFinal;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.trees.TreeNode;

import java.util.function.Function;

/**
 * Created by kenya on 2019/3/8.
 */
public class Errors {
//    public static class TreeNodeException<TreeType extends TreeNode> extends Exception{
//        transient TreeType tree;
//        public TreeNodeException(TreeType tree,
//                String msg,
//                Throwable cause){
//            super(msg,cause);
//            this.tree = tree;
//        }
//
//        // Yes, this is the same as a default parameter, but... those don't seem to work with SBT
//        // external project dependencies for some reason.
//        public TreeNodeException(TreeType tree, String msg){
//            this(tree, msg,null);
//        }
//
//        @Override
//        public String getMessage(){
//                return "${super.getMessage}, tree:${if (treeString contains ";
//        }
//    }

    public static <TreeType extends TreeNode,A>  A attachTree(TreeType tree, String msg, Function<Void,A>f){
        try {
            return f.apply(null);
        }catch(Exception e) {
            if(e instanceof NonFinal){
                //TODO
                // SPARK-16748: We do not want SparkExceptions from job failures in the planning phase
                // to create TreeNodeException. Hence, wrap exception only if it is not SparkException.
//                case NonFatal(e) if !e.isInstanceOf[SparkException] =>
//                    throw new TreeNodeException(tree, msg, e)
            }

        }
        return null;
    }
}
