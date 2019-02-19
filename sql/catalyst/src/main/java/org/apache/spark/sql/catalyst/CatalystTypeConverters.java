package org.apache.spark.sql.catalyst;

import org.apache.spark.sql.types.*;

import java.util.function.Function;

/**
 * Created by kenya on 2019/1/24.
 */
public class CatalystTypeConverters {

    private static boolean isPrimitive(DataType dataType){
        if(dataType instanceof BooleanType
            || dataType instanceof ByteType
            || dataType instanceof ShortType
            || dataType instanceof IntegerType
            || dataType instanceof LongType
            || dataType instanceof DoubleType
                ){
            return true;
        }else{
            return false;
        }
    }

    public static Function<Object,Object> createToCatalystConverter(DataType dataType){
        if (isPrimitive(dataType)) {
            // Although the `else` branch here is capable of handling inbound conversion of primitives,
            // we add some special-case handling for those types here. The motivation for this relates to
            // Java method invocation costs: if we have rows that consist entirely of primitive columns,
            // then returning the same conversion function for all of the columns means that the call site
            // will be monomorphic instead of polymorphic. In microbenchmarks, this actually resulted in
            // a measurable performance impact. Note that this optimization will be unnecessary if we
            // use code generation to construct Scala Row -> Catalyst Row converters.
            Function<Object,Object>convert = new Function<Object, Object>() {
                @Override
                public Object apply(Object maybeScalaValue){
                    return maybeScalaValue;
                }

            }
            return convert;
      } else {
            getConverterForType(dataType).toCatalyst
        }
    }

    getConverterForType(dataType: DataType): CatalystTypeConverter[Any, Any, Any] = {
        val converter = dataType match {
            case udt: UserDefinedType[_] => UDTConverter(udt)
            case arrayType: ArrayType => ArrayConverter(arrayType.elementType)
            case mapType: MapType => MapConverter(mapType.keyType, mapType.valueType)
            case structType: StructType => StructConverter(structType)
            case StringType => StringConverter
            case DateType => DateConverter
            case TimestampType => TimestampConverter
            case dt: DecimalType => new DecimalConverter(dt)
            case BooleanType => BooleanConverter
            case ByteType => ByteConverter
            case ShortType => ShortConverter
            case IntegerType => IntConverter
            case LongType => LongConverter
            case FloatType => FloatConverter
            case DoubleType => DoubleConverter
            case dataType: DataType => IdentityConverter(dataType)
        }
        converter.asInstanceOf[CatalystTypeConverter[Any, Any, Any]]
    }
}
