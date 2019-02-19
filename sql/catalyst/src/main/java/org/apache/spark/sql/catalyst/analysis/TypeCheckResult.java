package org.apache.spark.sql.catalyst.analysis;

/**
 * Created by kenya on 2019/1/22.
 */
public abstract class TypeCheckResult {
    public abstract boolean isSuccess();
    public boolean isFailure(){
        return !isSuccess();
    }

    /**
     * Created by kenya on 2019/1/22.
     */
    public static class TypeCheckSuccess extends TypeCheckResult {
        @Override
        public boolean isSuccess(){
            return true;
        }
    }

    /**
     * Created by kenya on 2019/1/22.
     */
    public static class TypeCheckFailure extends TypeCheckResult {
        String message;

        public TypeCheckFailure( String message){
            this.message = message;
        }

        @Override
        public boolean isSuccess(){
            return false;
        }
    }
}
