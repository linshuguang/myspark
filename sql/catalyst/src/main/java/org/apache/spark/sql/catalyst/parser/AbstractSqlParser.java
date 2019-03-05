package org.apache.spark.sql.catalyst.parser;

import jdk.nashorn.internal.parser.Lexer;
import org.antlr.v4.runtime.CharStreams;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;
import static org.apache.spark.sql.catalyst.parser.ParserDriver.*;
/**
 * Created by kenya on 2019/2/28.
 */
public abstract class AbstractSqlParser implements ParserInterface{
    public static Logger LOGGER = LoggerFactory.getLogger(AbstractSqlParser.class);

    @Override
    public DataType parseDataType(String sqlText): DataType = parse(sqlText) { parser =>
        astBuilder.visitSingleDataType(parser.singleDataType())
    }

    protected <T> T parse(String command,Function<SqlBaseParser,T>toResult){
        LOGGER.debug("Parsing command: {}",command);

        ParserDriver
        SqlBaseLexer lexer = new SqlBaseLexer(new UpperCaseCharStream(CharStreams.fromString(command)));
        lexer.removeErrorListeners();
        lexer.addErrorListener(new ParseErrorListener());
        lexer.legacy_setops_precedence_enbled = SQLConf.get().setOpsPrecedenceEnforced

        val tokenStream = new CommonTokenStream(lexer)
        val parser = new SqlBaseParser(tokenStream)
        parser.addParseListener(PostProcessor)
        parser.removeErrorListeners()
        parser.addErrorListener(ParseErrorListener)
        parser.legacy_setops_precedence_enbled = SQLConf.get.setOpsPrecedenceEnforced

        try {
            try {
                // first, try parsing with potentially faster SLL mode
                parser.getInterpreter.setPredictionMode(PredictionMode.SLL)
                toResult(parser)
            }
            catch {
                case e: ParseCancellationException =>
                    // if we fail, parse with LL mode
                    tokenStream.seek(0) // rewind input stream
                    parser.reset()

                    // Try Again.
                    parser.getInterpreter.setPredictionMode(PredictionMode.LL)
                    toResult(parser)
            }
        }
        catch {
            case e: ParseException if e.command.isDefined =>
                throw e
            case e: ParseException =>
                throw e.withCommand(command)
            case e: AnalysisException =>
                val position = Origin(e.line, e.startPosition)
                throw new ParseException(Option(command), e.message, position, position)
        }
    }
}
