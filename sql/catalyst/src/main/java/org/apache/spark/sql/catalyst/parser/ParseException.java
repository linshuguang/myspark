package org.apache.spark.sql.catalyst.parser;

import lombok.Data;
import org.antlr.v4.runtime.ParserRuleContext;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.catalyst.trees.TreeNode;

/**
 * Created by kenya on 2019/1/20.
 */
@Data
public class ParseException extends AnalysisException{
    String command;
    TreeNode.Origin start;
    TreeNode.Origin stop;

    public ParseException(String message, ParserRuleContext ctx){
        this(ParserUtils.command(ctx), message, ParserUtils.position(ctx.getStart()), ParserUtils.position(ctx.getStop()));
    }

    public ParseException(String command, String message, TreeNode.Origin start, TreeNode.Origin stop){
        super(message);
        this.command = command;
        this.start  = start;
        this.stop = stop;
    }
    public ParseException withCommand(String cmd){
        return new ParseException(cmd, getMessage(), start, stop);
    }

}
