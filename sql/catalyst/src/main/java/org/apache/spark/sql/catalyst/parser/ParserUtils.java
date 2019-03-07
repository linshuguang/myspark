package org.apache.spark.sql.catalyst.parser;

import javafx.util.Pair;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.ParserRuleContext;

import java.io.InputStream;
import java.math.BigDecimal;
import java.util.*;
import java.util.function.Function;

import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.trees.TreeNode;
import org.apache.spark.sql.catalyst.trees.TreeNode.CurrentOrigin;
import org.apache.spark.sql.catalyst.trees.TreeNode.Origin;
/**
 * Created by kenya on 2019/1/18.
 */
public class ParserUtils {

    public static boolean NonFatal(Throwable t){
        if(t instanceof VirtualMachineError
                || t instanceof ThreadDeath
                || t instanceof InterruptedException
                || t instanceof LinkageError){
            return false;
        }else{
            return true;
        }
    }
    public static boolean isValidInt(BigDecimal bd) {
        return bd.signum() == 0 || bd.scale() <= 0 || bd.stripTrailingZeros().scale() <= 0;
    }

    public static <T, R extends ParserRuleContext> T withOrigin(R ctx, Function<R, T>f){

        Origin current = CurrentOrigin.get();
        CurrentOrigin.set(position(ctx.getStart()));
        T ret;
        try {
            ret = f.apply(ctx);
        } finally {
            CurrentOrigin.set(current);
        }
        return ret;
    }

    @FunctionalInterface
    interface MyReduceFunctionalInterface<T> {

        public T apply(T A1, T A2);
    }
    public static <T> T reduce(List<T> lists, MyReduceFunctionalInterface<T>f){
        T t = lists.get(0);
        for(int i=1;i<lists.size();i++){
            t = f.apply(t, lists.get(i));
        }
        return t;
    }

    public static void require(boolean requirement, Object message){
        if(!requirement){
            throw new RuntimeException(message.toString());
        }
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
                    enclosure = new Character(currentChar);
                }
            } else if (enclosure == currentChar) {
                enclosure = null;
            } else if (currentChar == '\\') {

                if ((i + 6 < strLength) && b.charAt(i + 1) == 'u') {
                    // \u0000 style character literals.

                    int base = i + 2;
                    //ParserUtils.foldLeft()
                    int code = foldLeft(range(0,4),0,(mid,j)->{
                        int digit = Character.digit(b.charAt(j + base), 16);
                        return (mid << 4) + digit;
                    });
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


    public static void operationNotAllowed(String message, ParserRuleContext ctx){
        throw new ParseException("Operation not allowed: "+message, ctx);
    }

    public static String command(ParserRuleContext ctx){
        CharStream stream = ctx.getStart().getInputStream();
        return stream.getText(Interval.of(0, stream.size() - 1));
    }

    public static Origin position(Token token){
        if(token==null){
            return new Origin();
        }
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
    public static void validate(boolean f, String message, ParserRuleContext ctx){
        if (!f) {
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

    public static LogicalPlan optional(LogicalPlan plan, Object ctx,MyMapFunctionalInterface<Object>f){
        if (ctx != null) {
            return f.apply(ctx, plan);
        } else {
            return plan;
        }
    }

    public static <C, T> List<T> reverseMap(
            List<C> ctxs,
            Function<C,T>f){

        List<T> tList = map(ctxs,f);
        Collections.reverse(tList);
        return tList;
    }

    public static <C, T > List<T> map(
            List<C> ctxs,
            Function<C,T>f

    ){
        List<T> tList = new ArrayList<>();
      for(C ctx: ctxs){
          tList.add(f.apply(ctx));
      }
      return tList;
    }






    public static <C, T > List<T> flatMap(
            List<C> ctxs,
            Function<C,List<T>>f

    ){
        List<T> tList = new ArrayList<>();
        for(C ctx: ctxs){
            tList.addAll(f.apply(ctx));
        }
        return tList;
    }

    @FunctionalInterface
    public interface FoldLeftFunctionalInterface <T, C>{

        public T apply(T left, C right);
    }

    public static <T, C> T foldLeft(Collection<C> collection, T t, FoldLeftFunctionalInterface<T, C> f ){
        T initial = t;
        for( C col : collection){
            t = f.apply(t, col);
        }
        return t;
    }

    public static List<Integer> range(int from, int to){
        List<Integer> ret = new ArrayList<>();
        for(int i=from; i <to;i++){
            ret.add(i);
        }
        return ret;
    }

    public static <T, C> T foldRight(Collection<C> collection, T t, FoldLeftFunctionalInterface<T, C> f ){
        T initial = t;

        C[] array = (C[])collection.toArray();
        for(int i = collection.size()-1; i>=0;i--){
            C col = array[i];
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

    public static <K,V> Map<K,V> toMap(List<Pair<K,V>> pairs){
        Map<K,V> map = new HashMap<>();
        for(Pair<K,V> pair:pairs){
            map.put(pair.getKey(),pair.getValue());
        }
        return map;
    }

    public static <T> List<T> Seq(T ...t){
        return Arrays.asList(t);
    }

    public static String source(ParserRuleContext ctx){
        CharStream stream = ctx.getStart().getInputStream();
        return stream.getText(Interval.of(ctx.getStart().getStartIndex(), ctx.getStop().getStopIndex()));
    }

    public static String remainder(ParserRuleContext ctx){
        return remainder(ctx.getStop());
    }

    public static String remainder(Token token){
        CharStream stream = token.getInputStream();
        Interval interval = Interval.of(token.getStopIndex() + 1, stream.size() - 1);
        return stream.getText(interval);
    }

    public static <T> void checkDuplicateKeys(List<Pair<String,T>> keyPairs, ParserRuleContext ctx){

        Map<String,Integer> hash = new HashMap<>();
        for(Pair<String,T>keyPair:keyPairs){
            String key = keyPair.getKey();
            if(!hash.containsKey(key)){
                hash.put(key,0);
            }
            hash.put(key,1+hash.get(key));
            if(hash.get(key)>1){
                throw new ParseException("Found duplicate keys '"+key+"'.", ctx);
            }
        }
    }

    public static boolean equals(Object l, Object r){

        if(l==r){
            return true;
        }

        if(l==null && r==null){
            return true;
        }
        if(l!=null){
            return l.equals(r);
        }else{
            return r.equals(l);
        }
    }

}
