package org.apache.spark.sql.catalyst.plans.joinTypes;

import lombok.Data;

/**
 * Created by kenya on 2019/1/21.
 */
@Data
public class InnerLike extends JoinType {
    protected boolean explicitCartesian;
}
