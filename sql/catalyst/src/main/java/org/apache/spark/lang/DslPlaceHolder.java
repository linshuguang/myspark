package org.apache.spark.lang;

import lombok.Data;
import org.apache.commons.math3.analysis.function.Exp;
import org.apache.spark.sql.catalyst.expressions.Expression;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by kenya on 2019/3/12.
 */
@Data
public class DslPlaceHolder extends Expression{
    String name;

    public DslPlaceHolder(String name){
        this.name = name;
    }

    @Override
    public List<Expression> children(){
        return new ArrayList<>();
    }
}
