package com.cj.flink.sql.launcher;

import org.apache.flink.client.program.ClusterClient;

import com.cj.flink.sql.option.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterClientFactory {
    private static final Logger LOG = LoggerFactory.getLogger(ClusterClientFactory.class);

    private static final String HA_CLUSTER_ID = "high-availability.cluster-id";

    private static final String HIGH_AVAILABILITY = "high-availability";

    private static final String NODE = "NONE";

    private static final String ZOOKEEPER = "zookeeper";

    private static final String HADOOP_CONF = "fs.hdfs.hadoopconf";

    public static ClusterClient createClusterClient(Options launcherOptions) throws Exception {
        return null;
    }
}
