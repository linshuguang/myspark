package org.apache.spark.sql.types;

import org.apache.spark.sql.AnalysisException;

import java.math.BigDecimal;

/**
 * Created by kenya on 2019/1/22.
 */
public class DecimalType extends AbstractDataType {

    public static int MAX_PRECISION = 38;
    public static int MAX_SCALE = 38;
    //val SYSTEM_DEFAULT: DecimalType = DecimalType(MAX_PRECISION, 18)
    //val USER_DEFAULT: DecimalType = DecimalType(10, 0)
    public static int MINIMUM_ADJUSTED_SCALE = 6;

    public static DecimalType USER_DEFAULT = new DecimalType(10, 0);

    int precision;
    int scale;

    public DecimalType(int precision, int scale){
        if(scale>precision){
            throw new AnalysisException(
                    "Decimal scale ($scale) cannot be greater than precision ($precision).");
        }
        if (precision > DecimalType.MAX_PRECISION) {
            throw new AnalysisException(
                    "${DecimalType.simpleString} can only support precision up to ${DecimalType.MAX_PRECISION}");
        }

    }

    public static DecimalType SYSTEM_DEFAULT(){
        return new DecimalType(MAX_PRECISION, 18);
    }

    public static DecimalType USER_DEFAULT(){
        return new DecimalType(10, 0);
    }

    public static DecimalType fromBigDecimal(BigDecimal d){
        return new DecimalType(Math.max(d.precision(), d.scale()), d.scale());
    }

    @Override
    public boolean equals(Object o){
        boolean ok = super.equals(o);
        if(ok && o instanceof DecimalType){
            DecimalType d = (DecimalType)o;
            return d.precision==this.precision && d.scale==this.scale;
        }
        return ok;
    }

}
