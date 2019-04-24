package org.apache.spark.sql.catalyst.rules;

import org.apache.spark.sql.catalyst.trees.TreeNode;

/**
 * Created by kenya on 2019/4/7.
 */
public abstract class Rule<TreeType extends TreeNode> {

    String ruleName;

    public Rule(){
        String className = getClass().getName();
        if(className.endsWith("$")){
            ruleName = className.substring(0,className.length()-1);
        }else{
            ruleName = className;
        }
    }

    public abstract TreeType apply(TreeType plan);

}
