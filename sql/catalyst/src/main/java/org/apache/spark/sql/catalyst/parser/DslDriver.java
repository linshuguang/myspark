package org.apache.spark.sql.catalyst.parser;

import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.Interval;
import org.apache.spark.lang.Symbol;
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute;
import org.apache.spark.sql.catalyst.dsl.DSLLexer;
import org.apache.spark.sql.catalyst.dsl.DSLParser;

/**
 * Created by kenya on 2019/3/11.
 */
public class DslDriver {

    public static class DslCharStream implements CharStream {
        CodePointCharStream wrapped;

        public DslCharStream(CodePointCharStream wrapped) {
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

    private DSLLexer getLexer(String command) {
        CharStream input = new DslCharStream(CharStreams.fromString(command));
        DSLLexer lexer = new DSLLexer(input);
        return lexer;
    }


    private TokenStream getTokens(String inputString) {
        DSLLexer lexer = getLexer(inputString);
        TokenStream tokens = new CommonTokenStream(lexer);
        return tokens;
    }

    private DSLParser getParser(String code){
        TokenStream tokens = getTokens(code);
        DSLParser parser = new DSLParser(tokens);
        return parser;
    }

    private Object expression(String code){
        DSLParser parser = getParser(code);
        DSLParser.ExpressionContext context = parser.expression();
        return context.value;
    }


    public Object implicit(Object o){
        if(o instanceof Symbol){
            return symbolToUnresolvedAttribute((Symbol)o);
        }else if(o instanceof String){
            return expression((String)o);
        }
        return o;
    }

    public UnresolvedAttribute symbolToUnresolvedAttribute(Symbol s){
        return new UnresolvedAttribute(s.name);
    }

}
