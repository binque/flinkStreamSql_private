package com.cj.flink.sql.launcher;

import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.cj.flink.sql.Main;
import com.cj.flink.sql.constrant.ConfigConstrant;
import com.cj.flink.sql.enums.ClusterMode;
import com.cj.flink.sql.launcher.perjob.PerJobSubmitter;
import com.cj.flink.sql.option.OptionParser;
import com.cj.flink.sql.option.Options;
import com.cj.flink.sql.util.PluginUtil;
import com.google.common.collect.Lists;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URLDecoder;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;


/**
 * eg
 *
 * sh submit.sh -sql D:\sideSql.txt
 * -name xctest
 * -remoteSqlPluginPath /opt/dtstack/150_flinkplugin/sqlplugin
 * -localSqlPluginPath D:\gitspace\flinkStreamSQL\plugins
 * -addjar \["udf.jar\"\]
 * -mode yarn
 * -flinkconf D:\flink_home\kudu150etc
 * -yarnconf D:\hadoop\etc\hadoopkudu
 * -confProp \{\"time.characteristic\":\"EventTime\",\"sql.checkpoint.interval\":10000\}
 * -yarnSessionConf \{\"yid\":\"application_1564971615273_38182\"}
 */

public class LauncherMain {

    private static final String CORE_JAR = "core";

    /**
     * 根据不同运行环境，取得路径的分隔符
     */
    private static String SP = File.separator;

    private static String getLocalCoreJarPath(String localSqlRootJar) throws Exception {
        String jarPath = PluginUtil.getCoreJarFileName(localSqlRootJar, CORE_JAR);
        return localSqlRootJar + SP + jarPath;
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 1 && args[0].endsWith(".json")){
            args = parseJson(args);
        }
        OptionParser optionParser = new OptionParser(args);
        Options launcherOptions = optionParser.getOptions();

        //运行模式
        String mode = launcherOptions.getMode();

        //["-sql", "select * from test", "-mode", "yarn"]
        List<String> argList = optionParser.getProgramExeArgList();

        //{\"time.characteristic\":\"EventTime\",\"sql.checkpoint.interval\":10000\}
        //flink 任务的配置参数
        String confProp = launcherOptions.getConfProp();
        confProp = URLDecoder.decode(confProp, Charsets.UTF_8.toString());
        Properties confProperties = PluginUtil.jsonStrToObject(confProp, Properties.class);

        if (mode.equals(ClusterMode.local.name())){
            String[] localArgs = argList.toArray(new String[argList.size()]);
            Main.main(localArgs);
            return;
        }

        String pluginRoot = launcherOptions.getLocalSqlPluginPath();
        File jarFile = new File(getLocalCoreJarPath(pluginRoot));
        String[] remoteArgs = argList.toArray(new String[argList.size()]);
        PackagedProgram program = new PackagedProgram(jarFile, Lists.newArrayList(), remoteArgs);
        String savePointPath = confProperties.getProperty(ConfigConstrant.SAVE_POINT_PATH_KEY);
        if(StringUtils.isNotBlank(savePointPath)){
            String allowNonRestoredState = confProperties.getOrDefault(ConfigConstrant.ALLOW_NON_RESTORED_STATE_KEY, "false").toString();
            program.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(savePointPath, BooleanUtils.toBoolean(allowNonRestoredState)));
        }

        if (mode.equals(ClusterMode.yarnPer.name())){
            String flinkConfDir = launcherOptions.getFlinkconf();
            //主要就是去解析flink-conf.yaml
            Configuration config = StringUtils.isEmpty(flinkConfDir) ? new Configuration() : GlobalConfiguration.loadConfiguration(flinkConfDir);
            JobGraph jobGraph = PackagedProgramUtils.createJobGraph(program, config, 1);
            PerJobSubmitter.submit(launcherOptions, jobGraph, config);
        }else {
            ClusterClient clusterClient = ClusterClientFactory.createClusterClient(launcherOptions);
            clusterClient.run(program, 1);
            clusterClient.shutdown();
        }

    }


    private static String[] parseJson(String[] args) {
        BufferedReader reader = null;
        String lastStr = "";
        try{
            FileInputStream fileInputStream = new FileInputStream(args[0]);
            InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream, "UTF-8");
            reader = new BufferedReader(inputStreamReader);
            String tempString = null;
            while ((tempString = reader.readLine()) != null){
                lastStr += tempString;
            }
            reader.close();
        }catch (IOException e){
            e.printStackTrace();
        }finally {
            if (reader != null){
                try{
                    reader.close();
                }catch (IOException e){
                    e.printStackTrace();
                }
            }
        }

        Map<String,Object> map = JSON.parseObject(lastStr, new TypeReference<Map<String,Object>>(){});
        List<String> list = new LinkedList<>();

        for (Map.Entry<String, Object> entry: map.entrySet()) {
            list.add("-" + entry.getKey());
            list.add(entry.getValue().toString());
        }

        String[] array = list.toArray(new String[list.size()]);
        return array;
    }

}
