package org.apache.spark.sql.catalyst.expressions;

import org.apache.spark.sql.types.DataType;

/**
 * Created by kenya on 2019/1/23.
 */
public interface SpecializedGetters {
    Object get(int ordinal, DataType dataType);
}
