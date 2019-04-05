package org.apache.spark.sql.catalyst.plans.joinTypes;

import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/**
 * Created by kenya on 2019/1/21.
 */
public class JoinType {

    protected String sql;

    protected void require( JoinType joinType, String error,Class...types){
        List<Class> jts = Arrays.asList(types);
        for(Class jt: jts ){
            if(joinType.getClass()==jt){
                return;
            }
        }
        throw new RuntimeException(error);
    }

    public static JoinType build(String type){
        switch (type.toLowerCase(Locale.ROOT).replace("_","")){
            case "inner":
                return new Inner();
            case "outer":
            case "full":
            case "fullouter":
                return new FullOuter();
            case "leftouter":
            case "left":
                return new LeftOuter();
            case "rightouter":
            case "right":
                return new RightOuter();
            case "leftsemi":
                return new LeftSemi();
            case "leftanti":
                return new LeftAnti();
            case "cross":
                return new Cross();
            default:
                String[] supported = new String[]{
                        "inner",
                        "outer", "full", "fullouter", "full_outer",
                        "leftouter", "left", "left_outer",
                        "rightouter", "right", "right_outer",
                        "leftsemi", "left_semi",
                        "leftanti", "left_anti",
                        "cross"};
                    throw new IllegalArgumentException("Unsupported join type '$typ'. " +
                            "Supported join types include: " + StringUtils.join(supported) + ".");
        }

    }

    @Override
    public boolean equals(Object o){
        if(o instanceof JoinType){
            JoinType jt = (JoinType)o;
            return StringUtils.equals(sql,jt.sql);
        }
        return false;
    }
}
