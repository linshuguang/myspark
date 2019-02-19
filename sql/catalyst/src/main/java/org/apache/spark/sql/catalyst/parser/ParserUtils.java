package org.apache.spark.sql.catalyst.parser;

import javafx.util.Pair;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.ParserRuleContext;

import java.io.InputStream;
import java.util.*;
import java.util.function.Function;

import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.trees.TreeNode;
import org.apache.spark.sql.catalyst.trees.TreeNode.CurrentOrigin;
import org.apache.spark.sql.catalyst.trees.TreeNode.Origin;
/**
 * Created by kenya on 2019/1/18.
 */
public class ParserUtils {

    public static <T, R extends ParserRuleContext> T withOrigin(R ctx, Function<R, T>f){

        Origin current = CurrentOrigin.get();
        CurrentOrigin.set(position(ctx.getStart()));
        T ret;
        try {
            ret = f.apply(ctx);
        } finally {
            CurrentOrigin.set(current);
            ret = null;
        }
        return ret;
    }

    public static String string(Token token){
        return unescapeSQLString(token.getText());
    }

    public static String string(TerminalNode node){
        return unescapeSQLString(node.getText());
    }

    public static String  unescapeSQLString(String b){
        Character enclosure = null;
        StringBuilder sb = new StringBuilder(b.length());
        Function<Character,Object> appendEscapedChar = new Function<Character, Object>() {
            @Override
            public Object apply(Character character) {
                switch (character){
                    case '0': sb.append('\u0000'); break;
                    case '\'' : sb.append('\''); break;
                    case '"' : sb.append('\"'); break;
                    case 'b' : sb.append('\b'); break;
                    case 'n' : sb.append('\n'); break;
                    case 'r' : sb.append('\r'); break;
                    case 't' : sb.append('\t'); break;
                    case 'Z' : sb.append('\u001A'); break;
                    case '\\' : sb.append('\\'); break;
                        // The following 2 lines are exactly what MySQL does TODO: why do we do this?
                    case '%' : sb.append("\\%"); break;
                    case '_' : sb.append("\\_"); break;
                    default : sb.append(character); break;
                }
                return null;
            }
        };

        int i = 0;
        int strLength = b.length();
        while(i <strLength){
            char currentChar = b.charAt(i);
            if (enclosure == null) {
                if (currentChar == '\'' || currentChar == '\"') {
                    enclosure = currentChar;
                }
            } else if (enclosure == currentChar) {
                enclosure = null;
            } else if (currentChar == '\\') {

                if ((i + 6 < strLength) && b.charAt(i + 1) == 'u') {
                    // \u0000 style character literals.

                    int base = i + 2;
                    int code = 1;
//                    int code = (0 until 4).foldLeft(0) { (mid, j) =>
//                        val digit = Character.digit(b.charAt(j + base), 16)
//                        (mid << 4) + digit
//                    }
                    sb.append((char)code);
                    i += 5;
                } else if (i + 4 < strLength) {
                    // \000 style character literals.

                    char i1 = b.charAt(i + 1);
                    char i2 = b.charAt(i + 2);
                    char i3 = b.charAt(i + 3);

                    if ((i1 >= '0' && i1 <= '1') && (i2 >= '0' && i2 <= '7') && (i3 >= '0' && i3 <= '7')) {
                        char tmp = (char)((i3 - '0') + ((i2 - '0') << 3) + ((i1 - '0') << 6));
                        sb.append(tmp);
                        i += 3;
                    } else {
                        appendEscapedChar.apply(i1);
                        i += 1;
                    }
                } else if (i + 2 < strLength) {
                    // escaped character literals.
                    char n = b.charAt(i + 1);
                    appendEscapedChar.apply(n);
                    i += 1;
                }
            } else {
                // non-escaped character literals.
                sb.append(currentChar);
            }
            i += 1;
        }

        return sb.toString();
    }

    public static <T>void checkDuplicateKeys(List<Pair<String, T>>keyPairs, ParserRuleContext ctx) {
        HashSet<String> tmp =new HashSet<>();
        for(Pair<String, T> pair: keyPairs){
            if(tmp.contains(pair.getKey())){
                throw new ParseException("Found duplicate keys '$key'.", ctx);
            }
        }
    }

    public static String command(ParserRuleContext ctx){
        CharStream stream = ctx.getStart().getInputStream();
        return stream.getText(Interval.of(0, stream.size() - 1));
    }

    public static Origin position(Token token){
        return new Origin(token.getLine(), token.getCharPositionInLine());
    }

    @FunctionalInterface
    interface MyFunctionalInterface {

        public boolean validate();
    }

    public static void validate( MyFunctionalInterface  f, String message, ParserRuleContext ctx){
        if (!f.validate()) {
            throw new ParseException(message, ctx);
        }
    }


    @FunctionalInterface
    interface MyMapFunctionalInterface <T>{

        public LogicalPlan apply(T ctx, LogicalPlan plan);
    }
    /**
     * Map a [[LogicalPlan]] to another [[LogicalPlan]] if the passed context exists using the
     * passed function. The original plan is returned when the context does not exist.
     */
    public static <C> LogicalPlan optionalMap(LogicalPlan plan, C ctx, MyMapFunctionalInterface<C> f){
        if (ctx != null) {
            return f.apply(ctx, plan);
        } else {
            return plan;
        }
    }

    @FunctionalInterface
    interface FoldLeftFunctionalInterface <T, C>{

        public T apply(T left, C right);
    }

    public static <T, C> T foldLeft(Collection<C> collection, T t, FoldLeftFunctionalInterface<T, C> f ){
        T initial = t;
        for( C col : collection){
            t = f.apply(t, col);
        }
        return t;
    }


//    @FunctionalInterface
//    interface OptionalMapFunctionalInterface <C>{
//        public LogicalPlan apply(C ctx, LogicalPlan logicalPlan);
//    }
//    public static <C> LogicalPlan optionalMap(LogicalPlan plan, C ctx, OptionalMapFunctionalInterface<C>f){
//        if (ctx != null) {
//            return f.apply(ctx, plan);
//        } else {
//            return plan;
//        }
//    }
//    @FunctionalInterface
//    interface OptionalMapFunctionalInterface <C>{
//
//        public LogicalPlan apply(C left, LogicalPlan right);
//    }
//    public static <C> LogicalPlan optionalMap(LogicalPlan plan, C ctx, OptionalMapFunctionalInterface<C> f ){
//        if(ctx!=null){
//            return f.apply(ctx, plan);
//        }else{
//            return plan;
//        }
//    }




}
