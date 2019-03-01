package org.apache.spark.util;

import java.io.File;

/**
 * Created by kenya on 2019/2/26.
 */
public class ShutdownHookManager {

    // Register the path to be deleted via shutdown hook
    public static void registerShutdownDeleteDir(File file) {
        String absolutePath = file.getAbsolutePath();
        //TODO
//        shutdownDeletePaths.synchronized {
//            shutdownDeletePaths += absolutePath
//        }
    }

    public static void removeShutdownDeleteDir(File file) {
        String absolutePath = file.getAbsolutePath();
//        TODO
//        shutdownDeletePaths.synchronized {
//            shutdownDeletePaths.remove(absolutePath)
//        }
    }
}
