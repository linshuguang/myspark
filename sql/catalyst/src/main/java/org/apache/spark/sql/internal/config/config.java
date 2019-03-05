package org.apache.spark.sql.internal.config;

/**
 * Created by kenya on 2019/3/4.
 */
public class config {

    public static ConfigEntry AUTH_SECRET_FILE =
            new ConfigBuilder("spark.authenticate.secret.file")
                    .doc("Path to a file that contains the authentication secret to use. The secret key is " +
                            "loaded from this path on both the driver and the executors if overrides are not set for " +
                            "either entity (see below). File-based secret keys are only allowed when using " +
                            "Kubernetes.")
                    .stringConf()
                    .createOptional();
}
