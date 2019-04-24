package org.apache.spark.internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.impl.StaticLoggerBinder;
/**
 * Created by kenya on 2019/4/9.
 */
public class Logging {


    transient private Logger log_  = null;

    protected String logName(){
            // Ignore trailing $'s in the class names for Scala objects
            return this.getClass().getName();//this.getClass().getName().stripSuffix("$");
    }

    // Method to get or create the logger for this object
//    protected Logger log(){
//        if (log_ == null) {
//            initializeLogIfNecessary(false);
//            log_ = LoggerFactory.getLogger(logName)
//        }
//        log_
//    }
//
//    protected void initializeLogIfNecessary(Boolean isInterpreter){
//        initializeLogIfNecessary(isInterpreter, false);
//    }
//
//    protected Boolean initializeLogIfNecessary(
//            Boolean isInterpreter,
//            Boolean silent){
//        if (!Logging.initialized) {
//            Logging.initLock.synchronized {
//                if (!Logging.initialized) {
//                    initializeLogging(isInterpreter, silent)
//                    return true
//                }
//            }
//        }
//        false
//    }

    public static void logWarning(String msg) {
       // if (log.isWarnEnabled) log.warn(msg);
    }



//    private class Logging {
//  @volatile private var initialized = false
//  @volatile private var defaultRootLevel: Level = null
//  @volatile private var defaultSparkLog4jConfig = false
//        private val consoleAppenderToThreshold = new ConcurrentHashMap[ConsoleAppender, Priority]()
//
//        val initLock = new Object()
//        try {
//            // We use reflection here to handle the case where users remove the
//            // slf4j-to-jul bridge order to route their logs to JUL.
//            val bridgeClass = Utils.classForName("org.slf4j.bridge.SLF4JBridgeHandler")
//            bridgeClass.getMethod("removeHandlersForRootLogger").invoke(null)
//            val installed = bridgeClass.getMethod("isInstalled").invoke(null).asInstanceOf[Boolean]
//            if (!installed) {
//                bridgeClass.getMethod("install").invoke(null)
//            }
//        } catch {
//            case e: ClassNotFoundException => // can't log anything yet so just fail silently
//        }
//
//        /**
//         * Marks the logging system as not initialized. This does a best effort at resetting the
//         * logging system to its initial state so that the next class to use logging triggers
//         * initialization again.
//         */
//        def uninitialize(): Unit = initLock.synchronized {
//            if (isLog4j12()) {
//                if (defaultSparkLog4jConfig) {
//                    defaultSparkLog4jConfig = false
//                    LogManager.resetConfiguration()
//                } else {
//                    val rootLogger = LogManager.getRootLogger()
//                    rootLogger.setLevel(defaultRootLevel)
//                    rootLogger.getAllAppenders().asScala.foreach {
//                        case ca: ConsoleAppender =>
//                            ca.setThreshold(consoleAppenderToThreshold.get(ca))
//                        case _ => // no-op
//                    }
//                }
//            }
//            this.initialized = false
//        }
//
//    private def isLog4j12(): Boolean = {
//        // This distinguishes the log4j 1.2 binding, currently
//        // org.slf4j.impl.Log4jLoggerFactory, from the log4j 2.0 binding, currently
//        // org.apache.logging.slf4j.Log4jLoggerFactory
//        val binderClass = StaticLoggerBinder.getSingleton.getLoggerFactoryClassStr
//        "org.slf4j.impl.Log4jLoggerFactory".equals(binderClass)
//    }
//}
}
