package org.apache.spark.sql.catalyst.plans.logical.basicLogicalOperators;

import javafx.util.Pair;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.catalyst.parser.ParserUtils;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias;
import org.apache.spark.sql.catalyst.plans.logical.UnaryNode;
import org.apache.spark.sql.catalyst.util.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by kenya on 2019/1/20.
 */
public class With extends UnaryNode {
    List<Pair<String, SubqueryAlias>> cteRelations;

    public With(LogicalPlan child, List<Pair<String, SubqueryAlias>> cteRelations){
        super(child);
        this.cteRelations = cteRelations;
    }

    //@Override
    public String simpleString(){
        List<String> names = new ArrayList<>();
        for(Pair<String, SubqueryAlias> pair: cteRelations){
            names.add(pair.getKey());
        }
        String cteAliases = util.truncatedString(names, "[", ", ", "]");
        return "CTE " + cteAliases;
    }

    private Map<String, SubqueryAlias> convert(List<Pair<String, SubqueryAlias>> list){
        Map<String, SubqueryAlias> map = new HashMap<>();
        for(Pair<String, SubqueryAlias> pair:list){
            map.put(pair.getKey(),pair.getValue());
        }
        return map;
    }


    private <T> boolean emptyList(List<T> list){
        return list == null || list.size()==0;
    }


    @Override
    public boolean equals(Object o){
        if(o instanceof With){
            With w = (With)o;

            if(!ParserUtils.equals(getChild(),w.getChild())){
                return false;
            }

            boolean b1 = emptyList(cteRelations);
            boolean b2 = emptyList(w.cteRelations);
            if(b1&&b2){
                return true;
            }
            if((b1&&!b2)||(!b1&&b2)){
                return false;
            }

            Map<String, SubqueryAlias> m1 = convert(cteRelations);
            Map<String, SubqueryAlias> m2 = convert(w.cteRelations);
            if(m1.size()!=m2.size()){
                return false;
            }

            if(!ParserUtils.equals(m1.keySet(),m2.keySet())){
                return false;
            }

            for(String key: m1.keySet()){
                if(!ParserUtils.equals(m1.get(key),m2.get(key))){
                    return false;
                }
            }
            return true;
        }
        return false;
    }

}
