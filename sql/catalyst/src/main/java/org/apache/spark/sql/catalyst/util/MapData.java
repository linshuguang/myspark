package org.apache.spark.sql.catalyst.util;

import java.io.Serializable;

/**
 * Created by kenya on 2019/1/23.
 */
public abstract class MapData implements Serializable {
    public abstract int numElements();

    public abstract ArrayData keyArray();

    public abstract ArrayData valueArray();

    public abstract MapData copy();
}
