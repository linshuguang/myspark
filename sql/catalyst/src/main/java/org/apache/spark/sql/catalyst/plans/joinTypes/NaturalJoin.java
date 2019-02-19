package org.apache.spark.sql.catalyst.plans.joinTypes;

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


}
