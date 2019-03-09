package org.apache.spark.sql.catalyst.plans.logical;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by kenya on 2019/2/21.
 */
public abstract class LeafNode extends LogicalPlan {

    @Override
    protected final List<LogicalPlan> children(){
        return new ArrayList<>();
    }
}
