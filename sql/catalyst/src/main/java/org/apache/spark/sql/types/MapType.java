package org.apache.spark.sql.types;

import javafx.scene.chart.PieChart;
import lombok.Data;

/**
 * Created by kenya on 2019/1/19.
 */
@Data
public class MapType extends DataType {
    DataType keyType;
    DataType valueType;
    boolean containsNull;

    public MapType(DataType keyType, DataType valueType, boolean containsNull){
        this.keyType = keyType;
        this.valueType = valueType;
        this.containsNull = containsNull;
    }

    public MapType(DataType keyType, DataType valueType){
        this(keyType, valueType, true);
    }


}
