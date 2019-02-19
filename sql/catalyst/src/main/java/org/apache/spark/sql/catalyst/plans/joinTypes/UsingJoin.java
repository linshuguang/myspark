package org.apache.spark.sql.catalyst.plans.joinTypes;

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

}
