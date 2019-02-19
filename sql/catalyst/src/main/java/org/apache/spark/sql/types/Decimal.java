package org.apache.spark.sql.types;

import org.apache.spark.sql.catalyst.parser.ParserUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created by kenya on 2019/1/22.
 */
public class Decimal implements Comparable {
    private BigDecimal decimalVal = null;
    private Long longVal = 0L;
    private Integer _precision = 1;
    private Integer _scale = 0;


    /** Maximum number of decimal digits an Int can represent */


    /** Maximum number of decimal digits a Long can represent */
    private static int MAX_LONG_DIGITS = 18;

    private static int  ROUND_HALF_UP = BigDecimal.ROUND_HALF_UP;
    private static int  ROUND_HALF_EVEN = BigDecimal.ROUND_HALF_EVEN;
    private static int  ROUND_CEILING = BigDecimal.ROUND_CEILING;
    private static int  ROUND_FLOOR = BigDecimal.ROUND_FLOOR;

    public Decimal(BigDecimal value){
        set(value);
    }

    @Override
    public int compareTo(Object o) {
        return 0;
    }


    private static List<Long> range(int scope){
        int begin = 0;
        int end = scope;
        List<Long> ret = new ArrayList<>();
        for(int i=0; i < scope; i++){
            ret.add((long)Math.pow(10, i));
        }
        return ret;
    }


    private static List<Long> POW_10 ;
    static {
        POW_10 = range(MAX_LONG_DIGITS + 1);
    }

    public int precision() {
        return _precision;
    }

    public int scale(){
        return _scale;
    }

    public Decimal set(Long longVal){
        if (longVal <= -POW_10.get(MAX_LONG_DIGITS) || longVal >= POW_10.get(MAX_LONG_DIGITS)) {
            // We can't represent this compactly as a long without risking overflow
            this.decimalVal = new BigDecimal(longVal);
            this.longVal = 0L;
        } else {
            this.decimalVal = null;
            this.longVal = longVal;
        }
        this._precision = 20;
        this._scale = 0;
        return this;
    }

    public Decimal set(Integer intVal){
        this.decimalVal = null;
        this.longVal = Long.valueOf(intVal);
        this._precision = 10;
        this._scale = 0;
        return this;
    }

    public Decimal set(Long unscaled, Integer precision, Integer scale){
        if (setOrNull(unscaled, precision, scale) == null) {
            throw new IllegalArgumentException("Unscaled value too large for precision");
        }
        return this;
    }





    public Decimal setOrNull(Long unscaled, Integer precision, Integer scale){
        if (unscaled <= -POW_10.get(MAX_LONG_DIGITS) || unscaled >= POW_10.get(MAX_LONG_DIGITS)) {
            // We can't represent this compactly as a long without risking overflow
            if (precision < 19) {
                return null;  // Requested precision is too low to represent this value
            }
            //TODO:
            //this.decimalVal = BigDecimal(unscaled, scale);
            //this.decimalVal = new BigDecimal(unscaled, scale);
            this.longVal = 0L;
        } else {
            Long p = POW_10.get(Math.min(precision, MAX_LONG_DIGITS));
            if (unscaled <= -p || unscaled >= p) {
                return null;  // Requested precision is too low to represent this value
            }
            this.decimalVal = null;
            this.longVal = unscaled;
        }
        this._precision = precision;
        this._scale = scale;
        return this;
    }

    public Decimal set(BigDecimal decimal, Integer precision, Integer scale){
        this.decimalVal = decimal.setScale(scale, ROUND_HALF_UP);
        if(decimalVal.precision()>=precision){
            throw new RuntimeException("Decimal precision ${decimalVal.precision} exceeds max precision $precision");
        }
        this.longVal = 0L;
        this._precision = precision;
        this._scale = scale;
        return this;
    }

    public Decimal set(BigDecimal decimal){
        this.decimalVal = decimal;
        this.longVal = 0L;
        if (decimal.precision() <= decimal.scale()) {
            // For Decimal, we expect the precision is equal to or large than the scale, however,
            // in BigDecimal, the digit count starts from the leftmost nonzero digit of the exact
            // result. For example, the precision of 0.01 equals to 1 based on the definition, but
            // the scale is 2. The expected precision should be 3.
            this._precision = decimal.scale() + 1;
        } else {
            this._precision = decimal.precision();
        }
        this._scale = decimal.scale();
        return this;
    }

    public Decimal set(BigInteger bigintval){
        try {
            this.decimalVal = null;
            this.longVal = bigintval.longValueExact();
            this._precision = DecimalType.MAX_PRECISION;
            this._scale = 0;
            return this;
        } catch(ArithmeticException e) {
            return set(new BigDecimal(bigintval));
        }
    }

    public Decimal set(Decimal decimal){
        this.decimalVal = decimal.decimalVal;
        this.longVal = decimal.longVal;
        this._precision = decimal._precision;
        this._scale = decimal._scale;
        return this;
    }

    public BigDecimal toBigDecimal(){
        if (decimalVal!=null) {
            return decimalVal;
        } else {
            //TODO:
            //return new BigDecimal(new BigInteger(longVal.longValue()), _scale);
            //return new BigDecimal(Integer.valueOf(longVal), _scale);
            return null;
        }
    }
    @Override
    public String toString(){
        return toBigDecimal().toString();
    }

    public String toDebugString(){
        if (decimalVal!=null) {
            return "Decimal(expanded,$decimalVal,$precision,$scale})";
        } else {
            return "Decimal(compact,$longVal,$precision,$scale})";
        }
    }

    public Double toDouble(){
        return toBigDecimal().doubleValue();
    }

    public Float toFloat() {
        return toBigDecimal().floatValue();
    }

    public Long toLong(){
        if (decimalVal==null) {
            return longVal / POW_10.get(_scale);
        } else {
            return decimalVal.longValue();
        }
    }

    public Integer toInt(){
        return toLong().intValue();
    }

    public Short toShort() {
        return toLong().shortValue();
    }

    public Byte toByte(){
        return toLong().byteValue();
    }

}
