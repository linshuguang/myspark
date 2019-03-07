package org.apache.spark.sql.types;

import lombok.Data;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by kenya on 2019/1/19.
 */
@Data
public class Metadata implements Serializable{

    Map<String, Object> map;

    public Metadata(Map<String, Object> map){
        this.map = map;
    }

    public Metadata(){
        this(new HashMap<>());
    }

    public static Metadata empty(){
        return new Metadata(new HashMap<>());
    }

}
