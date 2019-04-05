package org.apache.spark.sql.catalyst.plans.joinTypes;

import org.apache.spark.sql.catalyst.parser.ParserUtils;

import java.util.List;

/**
 * Created by kenya on 2019/1/22.
 */
public class UsingJoin extends JoinType {
    JoinType tpe;
    List<String> usingColumns;

    public UsingJoin(JoinType tpe, List<String> usingColumns){
        require(tpe, "Unsupported using join type "+tpe,Inner.class, LeftOuter.class, LeftSemi.class, RightOuter.class, FullOuter.class, LeftAnti.class);
        this.tpe = tpe;
        this.usingColumns = usingColumns;
    }

    @Override
    public boolean equals(Object o){
        if(o instanceof UsingJoin){
            UsingJoin nj = (UsingJoin)o;
            if(!ParserUtils.equals(tpe,nj.tpe)){
                return false;
            }
            return super.equals(o);
        }
        return false;
    }
}
