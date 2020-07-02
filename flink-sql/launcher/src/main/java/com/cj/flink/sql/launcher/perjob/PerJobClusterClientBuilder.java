package com.cj.flink.sql.launcher.perjob;

import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.yarn.AbstractYarnClusterDescriptor;
import org.apache.flink.yarn.YarnClusterDescriptor;

import com.cj.flink.sql.enums.EPluginLoadMode;
import com.cj.flink.sql.launcher.YarnConfLoader;
import com.cj.flink.sql.option.Options;
import com.esotericsoftware.minlog.Log;
import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class PerJobClusterClientBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(PerJobClusterClientBuilder.class);

    private static final String DEFAULT_CONF_DIR = "./";

    private YarnClient yarnClient;

    private YarnConfiguration yarnConf;

    private Configuration flinkConfig;

    public void init(String yarnConfDir, Configuration flinkConfig, Properties userConf) throws Exception {

        if(Strings.isNullOrEmpty(yarnConfDir)) {
            throw new RuntimeException("parameters of yarn is required");
        }
        userConf.forEach((key, val) -> flinkConfig.setString(key.toString(), val.toString()));
        this.flinkConfig = flinkConfig;
        SecurityUtils.install(new SecurityConfiguration(flinkConfig));

        yarnConf = YarnConfLoader.getYarnConf(yarnConfDir);
        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(yarnConf);
        yarnClient.start();

        Log.info("----init yarn success ----");
    }

    public AbstractYarnClusterDescriptor createPerJobClusterDescriptor(String flinkJarPath, Options launcherOptions, JobGraph jobGraph)  throws MalformedURLException {
        String flinkConf = StringUtils.isEmpty(launcherOptions.getFlinkconf()) ? DEFAULT_CONF_DIR : launcherOptions.getFlinkconf();
        AbstractYarnClusterDescriptor clusterDescriptor = getClusterDescriptor(flinkConfig, yarnConf, flinkConf);
        if (StringUtils.isNotBlank(flinkJarPath)) {
            if (!new File(flinkJarPath).exists()) {
                throw new RuntimeException("The param '-flinkJarPath' ref dir is not exist");
            }
        }

        List<File> shipFiles = new ArrayList<>();
        if (flinkJarPath != null) {
            File[] jars = new File(flinkJarPath).listFiles();
            for (File file : jars) {
                if (file.toURI().toURL().toString().contains("flink-dist")) {
                    clusterDescriptor.setLocalJarPath(new Path(file.toURI().toURL().toString()));
                } else {
                    shipFiles.add(file);
                }
            }
        } else {
            throw new RuntimeException("The Flink jar path is null");
        }

        // classpath , all node need contain plugin jar
        String pluginLoadMode = launcherOptions.getPluginLoadMode();
        if (StringUtils.equalsIgnoreCase(pluginLoadMode, EPluginLoadMode.CLASSPATH.name())) {
            fillJobGraphClassPath(jobGraph);
        } else if (StringUtils.equalsIgnoreCase(pluginLoadMode, EPluginLoadMode.SHIPFILE.name())) {
            List<File> pluginPaths = getPluginPathToShipFiles(jobGraph);
            shipFiles.addAll(pluginPaths);
        } else {
            throw new IllegalArgumentException("Unsupported plugin loading mode " + pluginLoadMode
                    + " Currently only classpath and shipfile are supported.");
        }
        clusterDescriptor.addShipFiles(shipFiles);
        clusterDescriptor.setName(launcherOptions.getName());
        String queue = launcherOptions.getQueue();
        if (!Strings.isNullOrEmpty(queue)) {
            clusterDescriptor.setQueue(queue);
        }
        return clusterDescriptor;
    }

    private static void fillJobGraphClassPath(JobGraph jobGraph) throws MalformedURLException {
        Map<String, DistributedCache.DistributedCacheEntry> jobCacheFileConfig = jobGraph.getUserArtifacts();
        for(Map.Entry<String,  DistributedCache.DistributedCacheEntry> tmp : jobCacheFileConfig.entrySet()){
            if(tmp.getKey().startsWith("class_path")){
                jobGraph.getClasspaths().add(new URL("file:" + tmp.getValue().filePath));
            }
        }
    }

    private List<File> getPluginPathToShipFiles(JobGraph jobGraph) {
        List<File> shipFiles = new ArrayList<>();
        Map<String, DistributedCache.DistributedCacheEntry> jobCacheFileConfig = jobGraph.getUserArtifacts();
        for(Map.Entry<String,  DistributedCache.DistributedCacheEntry> tmp : jobCacheFileConfig.entrySet()){
            if(tmp.getKey().startsWith("class_path")){
                shipFiles.add(new File(tmp.getValue().filePath));
            }
        }
        return shipFiles;
    }

    private AbstractYarnClusterDescriptor getClusterDescriptor(
            Configuration configuration,
            YarnConfiguration yarnConfiguration,
            String configurationDirectory) {
        return new YarnClusterDescriptor(
                configuration,
                yarnConfiguration,
                configurationDirectory,
                yarnClient,
                false);
    }

}
