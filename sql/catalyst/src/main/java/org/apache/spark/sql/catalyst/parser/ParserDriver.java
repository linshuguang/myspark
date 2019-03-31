package org.apache.spark.sql.catalyst.parser;

import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.TerminalNodeImpl;
import org.apache.spark.sql.catalyst.trees.TreeNode;

import java.util.function.Function;

import static org.apache.spark.sql.catalyst.parser.ParserUtils.*;
import static org.apache.spark.sql.catalyst.trees.TreeNode.*;
/**
 * Created by kenya on 2019/1/18.
 */
public class ParserDriver {


    public static class ParseErrorListener extends BaseErrorListener {

        @Override
        public void syntaxError(
                Recognizer recognizer,
                Object offendingSymbol,
                int line,
                int charPositionInLine,
                String msg,
                RecognitionException e) {
            Origin position = new Origin(line, charPositionInLine);
            throw new ParseException(null, msg, position, position);
        }
    }

    public static class UpperCaseCharStream implements CharStream {
        CodePointCharStream wrapped;

        public UpperCaseCharStream(CodePointCharStream wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public void consume() {
            wrapped.consume();
        }

        @Override
        public String getSourceName() {
            return wrapped.getSourceName();
        }

        @Override
        public int index() {
            return wrapped.index();
        }

        @Override
        public int mark() {
            return wrapped.mark();
        }

        @Override
        public void release(int marker) {
            wrapped.release(marker);
        }

        @Override
        public void seek(int where) {
            wrapped.seek(where);
        }

        @Override
        public int size() {
            return wrapped.size();
        }

        @Override
        public String getText(Interval interval) {
            // ANTLR 4.7's CodePointCharStream implementations have bugs when
            // getText() is called with an empty stream, or intervals where
            // the start > end. See
            // https://github.com/antlr/antlr4/commit/ac9f7530 for one fix
            // that is not yet in a released ANTLR artifact.
            if (size() > 0 && (interval.b - interval.a >= 0)) {
                return wrapped.getText(interval);
            } else {
                return "";
            }
        }

        @Override
        public int LA(int i) {
            int la = wrapped.LA(i);
            if (la == 0 || la == IntStream.EOF)
                return la;
            else
                return Character.toUpperCase(la);
        }
    }

    public static class PostProcessor extends SqlBaseBaseListener{

        @Override
        public void exitQuotedIdentifier(SqlBaseParser.QuotedIdentifierContext ctx){
            replaceTokenByIdentifier(ctx, 1, (token) ->{
                // Remove the double back ticks in the string.
                token.setText(token.getText().replace("``", "`"));
                return token;
            });
        }

        /** Treat non-reserved keywords as Identifiers. */
        @Override
        public void exitNonReserved(SqlBaseParser.NonReservedContext ctx){
            replaceTokenByIdentifier(ctx, 0,(c)->{return c;});
        }

        private void replaceTokenByIdentifier(
                ParserRuleContext ctx ,
                int stripMargins, Function<CommonToken,CommonToken>f){
            ParserRuleContext parent = ctx.getParent();
            parent.removeLastChild();
            Token token = (Token)ctx.getChild(0).getPayload();
            CommonToken newToken = new CommonToken(
                    new org.antlr.v4.runtime.misc.Pair(token.getTokenSource(), token.getInputStream()),
                    SqlBaseParser.IDENTIFIER,
                    token.getChannel(),
                    token.getStartIndex() + stripMargins,
                    token.getStopIndex() - stripMargins);
            parent.addChild(new TerminalNodeImpl(f.apply(newToken)));
        }
    }




}
