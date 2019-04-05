package org.apache.spark.sql.catalyst.plans.joinTypes;

import org.apache.spark.sql.catalyst.parser.ParserUtils;

import javax.print.attribute.standard.MediaSize;

/**
 * Created by kenya on 2019/1/22.
 */
public class NaturalJoin extends JoinType {

    JoinType tpe;

    public NaturalJoin(JoinType tpe){
        require(tpe, "Unsupported natural join type "+tpe,Inner.class, LeftOuter.class, RightOuter.class, FullOuter.class);
        this.tpe = tpe;
        this.sql = "NATURAL " + tpe.sql;
    }


    @Override
    public boolean equals(Object o){
        if(o instanceof NaturalJoin){
            NaturalJoin nj = (NaturalJoin)o;
            if(!ParserUtils.equals(tpe,nj.tpe)){
                return false;
            }
            return super.equals(o);
        }
        return false;
    }

}
