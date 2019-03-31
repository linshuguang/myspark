package org.apache.spark.sql.catalyst.plans.logical.basicLogicalOperators;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.catalyst.parser.ParserUtils;
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

    @Override
    public boolean equals(Object o){
        if(o instanceof InsertIntoTable){
            InsertIntoTable i = (InsertIntoTable)o;
            if(!ParserUtils.equals(table,i.table)){
                return false;
            }
            if(!ParserUtils.equals(query,i.query)){
                return false;
            }
            if(!(overwrite==i.overwrite && ifPartitionNotExists==i.ifPartitionNotExists)){
                return false;
            }

            if((partition==null&& i.partition!=null) || (partition!=null&& i.partition==null)){
                return false;
            }
            if(partition.keySet().size()!=i.partition.keySet().size()){
                return false;
            }
            for(String key: partition.keySet()){
                if(!StringUtils.equals(partition.get(key), i.partition.get(key))){
                    return false;
                }
            }
            return true;
        }
        return false;
    }

}
