package org.apache.spark.sql.catalyst.parser;

import org.antlr.v4.runtime.ParserRuleContext;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.catalyst.trees.TreeNode;

/**
 * Created by kenya on 2019/1/20.
 */
public class ParseException extends AnalysisException{
    String command;
    String message;
    TreeNode.Origin start;
    TreeNode.Origin stop;

    public ParseException(String message, ParserRuleContext ctx){
        this(ParserUtils.command(ctx), message, ParserUtils.position(ctx.getStart()), ParserUtils.position(ctx.getStop()));
    }

    public ParseException(String command, String message, TreeNode.Origin start, TreeNode.Origin stop){
        this.command = command;
        this.message = message;
        this.start  = start;
        this.stop = stop;
    }

}
