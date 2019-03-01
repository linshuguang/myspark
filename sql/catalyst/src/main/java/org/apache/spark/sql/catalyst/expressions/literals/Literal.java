package org.apache.spark.sql.catalyst.expressions.literals;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.LeafExpression;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;

/**
 * Created by kenya on 2019/1/22.
 */
public class Literal extends LeafExpression{

    Object value;
    DataType dataType;


    public static Literal TrueLiteral(){
        return new Literal(true, new BooleanType());
    }

    public static Literal FalseLiteral(){
        return new Literal(false, new BooleanType());
    }

    public Literal(Object value, DataType dataType){
        validateLiteralValue(value, dataType);
        this.value = value;
        this.dataType = dataType;
    }

    private void validateLiteralValue(Object value, DataType dataType){
        if(!doValidate(value, dataType)){
            throw new RuntimeException("Literal must have a corresponding value to ${dataType.catalogString}, " +
                    "but class ${Utils.getSimpleName(value.getClass)} found.");
        }
    }

    private boolean doValidate(Object v, DataType dataType){
        if(dataType==null){
            return true;
        }else if(dataType instanceof BooleanType){
            return v instanceof Boolean ;
        }else if(dataType instanceof ByteType){
            return v instanceof Byte;
        }else if(dataType instanceof ShortType) {
            return v instanceof Short;
        }else if(dataType instanceof IntegerType || dataType instanceof DateType){
            return v instanceof Integer;
        }else if(dataType instanceof LongType || dataType instanceof TimestampType) {
            return v instanceof Long;
        }else if(dataType instanceof FloatType) {
            return v instanceof Float;
        }else if(dataType instanceof DoubleType) {
            return v instanceof Double;
        }else if(dataType instanceof DecimalType) {
            return v instanceof Decimal;
        }else if( dataType instanceof CalendarIntervalType) {
            return v instanceof CalendarInterval;
        }else if(dataType instanceof BinaryType) {
            return v instanceof Byte[];
        }else if(dataType instanceof StructType ){
            return v instanceof UTF8String;
        }else if( dataType instanceof StructType){

            if(v instanceof InternalRow){
                StructType st = (StructType)dataType;
                InternalRow row = (InternalRow) v;
                int i = 0;
                for(StructField sf: st.getFields()){
                    DataType dt = sf.getDataType();
                    //TODO: check here
                    doValidate(row.get(i,dt), dt);
                    i++;
                }

            }else{
                return false;
            }
        }else if(dataType instanceof ArrayType){
            ArrayType at = (ArrayType)dataType;
            if(v instanceof ArrayData){
                ArrayData ar = (ArrayData) v;
                return ar.numElements()==0 || doValidate(ar.get(0,at.getElementType()), at.getElementType());
            }else{
                return false;
            }
        }else if(dataType instanceof MapType){
            MapType mt = (MapType)dataType;

            if(v instanceof MapData){
                MapData map = (MapData)v;
                return doValidate(map.keyArray(), new ArrayType(mt.getKeyType())) && doValidate(map.valueArray(), new ArrayType(mt.getValueType()));
            }else{
                return false;
            }

        }else if(dataType instanceof ObjectType){
            return ((ObjectType)dataType).getCls()==v.getClass();
        }else if(dataType instanceof UserDefinedType){
            UserDefinedType udt = (UserDefinedType)dataType;
            return doValidate(v, udt.getSqlType());
        }
        return false;
    }

    public static Literal build(Object v){
        DataType dataType = null;
        Object vv = v;
        if(v instanceof Integer){
            dataType = new IntegerType();
        }else if(v instanceof Long){
            dataType = new LongType();
        }else if(v instanceof Double){
            dataType = new DoubleType();
        }else if(v instanceof Float){
            dataType = new FloatType();
        }else if(v instanceof Byte){
            dataType = new ByteType();
        }else if(v instanceof Short){
            dataType = new ShortType();
        }else if(v instanceof String){
            vv = UTF8String.fromString(v.toString());
            dataType = new StringType();
        }else if(v instanceof Character){
            vv = UTF8String.fromString(v.toString());
            dataType = new StringType();
        }else if(v instanceof Boolean){
            dataType = new BooleanType();
        }else if(v instanceof BigDecimal){
            //vv = new Decimal((BigDecimal)v);
            BigDecimal d = (BigDecimal)v;
            dataType = new DecimalType(Math.max(d.precision(), d.scale()), d.scale());
        }else if(v instanceof Decimal){
            Decimal d = (Decimal)v;
            dataType = new DecimalType(Math.max(d.precision(), d.scale()), d.scale());
        }else if(v instanceof Timestamp){
            Timestamp t = (Timestamp)v;
            vv = DateTimeUtils.fromJavaTimestamp(t);
            dataType = new TimestampType();
        }else if(v instanceof Date){
            Date d = (Date)v;
            dataType = new DateType();
            vv = DateTimeUtils.fromJavaDate(d);
        }else if(v instanceof Byte[]){
            dataType = new ByteType();
        }else if(v instanceof Object[]){
            Object[] a = (Object[])v;
            DataType elementType = componentTypeToDataType(a.getClass().getComponentType());
             new  ArrayType(elementType);
        }

        return new Literal(vv, dataType);
    }

    private static DataType componentTypeToDataType(Class<?>clz){
        if(clz==Short.class){
            return new ShortType();
        }else if(clz==Integer.class){
            return new IntegralType();
        }else if(clz == Long.class){
            return new LongType();
        }else if(clz == Double.class){
            return new DoubleType();
        }else if(clz ==Byte.class){
            return new ByteType();
        }else if(clz == Float.class){
            return new FloatType();
        }else if(clz ==Boolean.class){
            return new BooleanType();
        }else if(clz==String.class){
            return new StringType();
        }else if(clz ==Timestamp.class){
            return new TimestampType();
        }else if(clz==BigDecimal.class){
            return DecimalType.SYSTEM_DEFAULT();
        }else if(clz== BigInteger.class){
            return DecimalType.SYSTEM_DEFAULT();
        }else if(clz==CalendarInterval.class){
            return new CalendarIntervalType();
        }else if(clz.isArray()){
            return new ArrayType(componentTypeToDataType(clz.getComponentType()));
        }else{
            throw new AnalysisException("Unsupported component type $clz in arrays");
        }
    }

}
