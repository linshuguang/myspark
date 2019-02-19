package org.apache.spark.sql.catalyst.util;

import org.apache.spark.sql.catalyst.expressions.SpecializedGetters;

import java.io.Serializable;

/**
 * Created by kenya on 2019/1/23.
 */
public abstract class ArrayData implements SpecializedGetters, Serializable {
    public abstract int numElements();
}
