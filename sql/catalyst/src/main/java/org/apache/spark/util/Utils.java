package org.apache.spark.util;

import org.apache.spark.network.util.JavaUtils;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

/**
 * Created by kenya on 2019/2/26.
 */
public class Utils {

    private static int MAX_DIR_CREATION_ATTEMPTS = 10;

    public static File createTempDir() throws Exception{
        return createTempDir(System.getProperty("java.io.tmpdir"), "spark");
    }

    public static File createTempDir(String root, String namePrefix) throws Exception{
        File dir = createDirectory(root, namePrefix);
        ShutdownHookManager.registerShutdownDeleteDir(dir);
        return dir;
    }

    public static File createDirectory(String root, String namePrefix) throws Exception{
        int attempts = 0;
        int maxAttempts = MAX_DIR_CREATION_ATTEMPTS;
        File dir = null;
        while (dir == null) {
        attempts += 1;
        if (attempts > maxAttempts) {
            throw new IOException("Failed to create a temp directory (under " + root + ") after " + maxAttempts + " attempts!");
        }
        try {
        dir = new File(root, namePrefix + "-" + UUID.randomUUID().toString());
        if (dir.exists() || !dir.mkdirs()) {
            dir = null;
        }
        } catch(Exception e) { if(e instanceof SecurityException )dir = null; }
        }

        return dir.getCanonicalFile();
        }


    public static void deleteRecursively(File file){
        if (file != null) {
            try {
                JavaUtils.deleteRecursively(file);
                ShutdownHookManager.removeShutdownDeleteDir(file);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    public static boolean isTesting(){
        return System.getenv().containsKey("SPARK_TESTING") || System.getProperties().containsKey("spark.testing");
    }


    public static void traverseUp(Object orig, Function<Class,Boolean> f){
        if(orig==null){
            return;
        }
        Object o = orig;
        Class c = o.getClass();
        while (c != Object.class){
            if(!f.apply(c)){
                break;
            }
            c = c.getSuperclass();
        }
    }



}
