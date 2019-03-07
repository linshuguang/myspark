package org.apache.spark;

import org.apache.spark.util.Utils;
import org.junit.runner.RunWith;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.File;
import java.util.function.Function;

/**
 * Created by kenya on 2019/2/26.
 */
public class SparkFunSuite {


    public interface TestFuncionInterface{
        void apply();
    }
    protected void test(String testName, TestFuncionInterface f){
        f.apply();
    }

    protected void withTempDir(Function<File,Void>f){
        File dir = null;
        try{
            dir = Utils.createTempDir();
            f.apply(dir);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            Utils.deleteRecursively(dir);
        }
    }
}
