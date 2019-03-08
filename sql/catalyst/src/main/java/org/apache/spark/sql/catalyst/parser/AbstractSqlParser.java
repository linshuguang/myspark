package org.apache.spark.sql.catalyst.parser;

import jdk.nashorn.internal.parser.Lexer;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.apache.spark.sql.AnalysisException;
import static org.apache.spark.sql.catalyst.trees.TreeNode.Origin;

import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.function.Function;
import static org.apache.spark.sql.catalyst.parser.ParserDriver.*;
/**
 * Created by kenya on 2019/2/28.
 */
public abstract class AbstractSqlParser implements ParserInterface{
    public static Logger LOGGER = LoggerFactory.getLogger(AbstractSqlParser.class);

    protected AstBuilder astBuilder;

    public AbstractSqlParser(SQLConf conf){
        this.astBuilder = new AstBuilder(conf);
    }

    @Override
    public DataType parseDataType(String sqlText){
        return parse(sqlText, (parser) ->{
            return astBuilder.visitSingleDataType(parser.singleDataType());
      });
    }

    public Expression parseExpression(String sqlText){
        return parse(sqlText, (parser) -> {
            return astBuilder.visitSingleExpression(parser.singleExpression());
        });
    }

    protected <T> T  parse(String command,Function<SqlBaseParser,T>toResult) {
        LOGGER.debug("Parsing command: {}", command);

        SqlBaseLexer lexer = new SqlBaseLexer(new UpperCaseCharStream(CharStreams.fromString(command)));
        lexer.removeErrorListeners();
        lexer.addErrorListener(new ParseErrorListener());
        lexer.legacy_setops_precedence_enbled = SQLConf.get().setOpsPrecedenceEnforced();

        CommonTokenStream tokenStream = new CommonTokenStream(lexer);

        //time consuming here
        SqlBaseParser parser = new SqlBaseParser(tokenStream);

        parser.addParseListener(new PostProcessor());
        parser.removeErrorListeners();
        parser.addErrorListener(new ParseErrorListener());
        parser.legacy_setops_precedence_enbled = SQLConf.get().setOpsPrecedenceEnforced();

        try {
            try {
                // first, try parsing with potentially faster SLL mode
                parser.getInterpreter().setPredictionMode(PredictionMode.SLL);
                return toResult.apply(parser);
            } catch (ParseCancellationException e) {
                // if we fail, parse with LL mode
                tokenStream.seek(0); // rewind input stream
                parser.reset();
                // Try Again.
                parser.getInterpreter().setPredictionMode(PredictionMode.LL);
                return toResult.apply(parser);
            }
        } catch (ParseException e) {
            if (e.getCommand() != null) {
                throw e;
            } else {
                throw e.withCommand(command);
            }
        } catch (AnalysisException e) {
            Origin position = new Origin(e.getLine(), e.getStartPosition());
            throw new ParseException(command, e.getMessage(), position, position);
        }
    }
}
