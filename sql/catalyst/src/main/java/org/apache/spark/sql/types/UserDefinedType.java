package org.apache.spark.sql.types;

import lombok.Data;

/**
 * Created by kenya on 2019/1/23.
 */
@Data
public  abstract class UserDefinedType<UserType> extends DataType {
        DataType sqlType;
        public abstract Object serialize(UserType obj);
}
