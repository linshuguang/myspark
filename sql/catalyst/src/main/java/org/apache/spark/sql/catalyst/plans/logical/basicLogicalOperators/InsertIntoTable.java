package org.apache.spark.sql.catalyst.plans.logical.basicLogicalOperators;

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by kenya on 2019/2/21.
 */
public class InsertIntoTable extends LogicalPlan {
    LogicalPlan table;
    Map<String ,String>partition;
    LogicalPlan query;
    boolean overwrite;
    boolean ifPartitionNotExists;

    public InsertIntoTable(LogicalPlan table,
            Map<String ,String>partition,
            LogicalPlan query,
            boolean overwrite,
            boolean ifPartitionNotExists){
        this.table = table;
        this.partition = partition;
        this.query = query;
        this.overwrite = overwrite;
        this.ifPartitionNotExists = ifPartitionNotExists;
    }

    @Override
    protected List<LogicalPlan> children(){
        LogicalPlan[] logicalPlans= new LogicalPlan[]{query};
        return Arrays.asList(logicalPlans);
    }

}
