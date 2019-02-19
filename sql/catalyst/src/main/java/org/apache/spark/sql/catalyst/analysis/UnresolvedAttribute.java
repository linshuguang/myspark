package org.apache.spark.sql.catalyst.analysis;

import org.apache.spark.sql.catalyst.expressions.Attribute;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by kenya on 2019/1/22.
 */
public class UnresolvedAttribute extends Attribute{

    List<String> nameParts;

    public UnresolvedAttribute(List<String> nameParts){
        this.nameParts = nameParts;
    }

    public UnresolvedAttribute(String name){
        nameParts = new ArrayList<>();
        nameParts.add(name);
    }

    public static UnresolvedAttribute quoted(String name) {
        return new UnresolvedAttribute(name);
    }

    public static UnresolvedAttribute apply(String name){
        return new UnresolvedAttribute(Arrays.asList(name.split("\\.")));
    }
}
