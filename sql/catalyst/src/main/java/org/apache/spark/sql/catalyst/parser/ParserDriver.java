package org.apache.spark.sql.catalyst.parser;

import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.Interval;
import org.apache.spark.sql.catalyst.trees.TreeNode;

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
}
