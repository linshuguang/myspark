package org.apache.spark.sql.catalyst.trees;

/**
 * Created by kenya on 2019/2/28.
 */
public abstract class Rule<TreeType extends TreeNode<TreeType>> {
    public abstract TreeType apply(TreeType plan);
}
