package com.cj.flink.sql.launcher.perjob;

import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.yarn.AbstractYarnClusterDescriptor;

import com.cj.flink.sql.option.Options;
import com.cj.flink.sql.util.PluginUtil;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URLDecoder;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class PerJobSubmitter {
    private static final Logger LOG = LoggerFactory.getLogger(PerJobSubmitter.class);

    public static String submit(Options launcherOptions, JobGraph jobGraph, Configuration flinkConfig) throws Exception {
        if (!StringUtils.isBlank(launcherOptions.getAddjar())) {
            String addjarPath = URLDecoder.decode(launcherOptions.getAddjar(), Charsets.UTF_8.toString());
            List<String> paths = getJarPaths(addjarPath);
            paths.forEach( path -> {
                jobGraph.addJar(new Path("file://" + path));
            });
        }

        String confProp = launcherOptions.getConfProp();
        confProp = URLDecoder.decode(confProp, Charsets.UTF_8.toString());
        Properties confProperties = PluginUtil.jsonStrToObject(confProp, Properties.class);
        ClusterSpecification clusterSpecification = FLinkPerJobResourceUtil.createClusterSpecification(confProperties);

        PerJobClusterClientBuilder perJobClusterClientBuilder = new PerJobClusterClientBuilder();
        perJobClusterClientBuilder.init(launcherOptions.getYarnconf(), flinkConfig, confProperties);

        String flinkJarPath = launcherOptions.getFlinkJarPath();
        AbstractYarnClusterDescriptor yarnClusterDescriptor = perJobClusterClientBuilder.createPerJobClusterDescriptor(flinkJarPath, launcherOptions, jobGraph);
        ClusterClient<ApplicationId> clusterClient = yarnClusterDescriptor.deployJobCluster(clusterSpecification, jobGraph,true);
        String applicationId = clusterClient.getClusterId().toString();
        String flinkJobId = jobGraph.getJobID().toString();

        String tips = String.format("deploy per_job with appId: %s, jobId: %s", applicationId, flinkJobId);
        LOG.info(tips);

        return applicationId;
    }

    private static List<String> getJarPaths(String addjarPath) {
        if (addjarPath.length() > 2) {
            addjarPath = addjarPath.substring(1,addjarPath.length()-1).replace("\"","");
        }
        List<String> paths = Arrays.asList(StringUtils.split(addjarPath, ","));
        return paths;
    }
}
