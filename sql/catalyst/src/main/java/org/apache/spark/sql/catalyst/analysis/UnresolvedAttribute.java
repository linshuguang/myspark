package org.apache.spark.sql.catalyst.analysis;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.parser.ParserUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by kenya on 2019/1/22.
 */
@Data
public class UnresolvedAttribute extends Attribute{

    public List<String> nameParts;

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

    public String name(){
        if(nameParts==null || nameParts.size()==0){
            return null;
        }
        StringBuilder sbuf = new StringBuilder();
        for(String namePart:nameParts){
            if(sbuf.length()>0){
                sbuf.append(".");
            }
            if(namePart.contains(".")){
                sbuf.append("`");
                sbuf.append(namePart);
                sbuf.append("`");
            }else{
                sbuf.append(namePart);
            }
        }
        return sbuf.toString();
    }

    @Override
    public boolean equals(Object o){
        if(o instanceof UnresolvedAttribute){
            UnresolvedAttribute u = (UnresolvedAttribute)o;
            return ParserUtils.equals(nameParts,u.nameParts);
        }
        return false;
    }

}
