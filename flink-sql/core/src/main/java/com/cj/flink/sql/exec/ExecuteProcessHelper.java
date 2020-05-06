package com.cj.flink.sql.exec;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import com.cj.flink.sql.enums.ClusterMode;
import com.cj.flink.sql.enums.EPluginLoadMode;
import com.cj.flink.sql.environment.MyLocalStreamEnvironment;
import com.cj.flink.sql.environment.StreamEnvConfigManager;
import com.cj.flink.sql.option.OptionParser;
import com.cj.flink.sql.option.Options;
import com.cj.flink.sql.parser.FlinkPlanner;
import com.cj.flink.sql.util.PluginUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class ExecuteProcessHelper {

    private static final String CLASS_FILE_NAME_FMT = "class_path_%d";
    private static final Logger LOG = LoggerFactory.getLogger(ExecuteProcessHelper.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static ParamsInfo parseParams(String[] args) throws Exception {
        LOG.info("------------program params-------------------------");
        System.out.println("------------program params-------------------------");
        Arrays.stream(args).forEach(arg -> LOG.info("{}", arg));
        Arrays.stream(args).forEach(System.out::println);
        LOG.info("-------------------------------------------");
        System.out.println("----------------------------------------");


        OptionParser optionParser = new OptionParser(args);
        Options options = optionParser.getOptions();

        String sql = URLDecoder.decode(options.getSql(), Charsets.UTF_8.name());
        String name = options.getName();
        String localSqlPluginPath = options.getLocalSqlPluginPath();
        String remoteSqlPluginPath = options.getRemoteSqlPluginPath();
        String pluginLoadMode = options.getPluginLoadMode();
        String deployMode = options.getMode();

        Preconditions.checkArgument(checkRemoteSqlPluginPath(remoteSqlPluginPath, deployMode, pluginLoadMode),
                "Non-local mode or shipfile deployment mode, remoteSqlPluginPath is required");

        String confProp = URLDecoder.decode(options.getConfProp(), Charsets.UTF_8.toString());
        Properties confProperties = PluginUtil.jsonStrToObject(confProp, Properties.class);
        List<URL> jarURList = getExternalJarUrls(options.getAddjar());

        return ParamsInfo.builder()
                .setSql(sql)
                .setName(name)
                .setLocalSqlPluginPath(localSqlPluginPath)
                .setRemoteSqlPluginPath(remoteSqlPluginPath)
                .setPluginLoadMode(pluginLoadMode)
                .setDeployMode(deployMode)
                .setConfProp(confProperties)
                .setJarUrlList(jarURList)
                .build();
    }

    private static List<URL> getExternalJarUrls(String addJarListStr) throws java.io.IOException {
        List<URL> jarUrlList = Lists.newArrayList();
        if (Strings.isNullOrEmpty(addJarListStr)) {
            return jarUrlList;
        }

        List<String> addJarFileList = OBJECT_MAPPER.readValue(URLDecoder.decode(addJarListStr, Charsets.UTF_8.name()), List.class);

        //Get External jar to load
        for (String addJarPath : addJarFileList) {
            jarUrlList.add(new File(addJarPath).toURI().toURL());
        }
        return jarUrlList;
    }

    /**
     * 非local模式或者shipfile部署模式，remoteSqlPluginPath必填
     *
     * @param remoteSqlPluginPath
     * @param deployMode
     * @param pluginLoadMode
     * @return
     */
    private static boolean checkRemoteSqlPluginPath(String remoteSqlPluginPath, String deployMode, String pluginLoadMode) {
        if (StringUtils.isEmpty(remoteSqlPluginPath)) {
            return StringUtils.equalsIgnoreCase(pluginLoadMode, EPluginLoadMode.SHIPFILE.name())
                    || StringUtils.equalsIgnoreCase(deployMode, ClusterMode.local.name());
        }
        return true;
    }

    /**
     * 构建执行的execution 任务
     * @param paramsInfo
     * @return
     * @throws Exception
     */
    public static StreamExecutionEnvironment getStreamExecution(ParamsInfo paramsInfo) throws Exception {
        StreamExecutionEnvironment env = ExecuteProcessHelper.getStreamExeEnv(paramsInfo.getConfProp(), paramsInfo.getDeployMode());
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        StreamQueryConfig streamQueryConfig = StreamEnvConfigManager.getStreamQueryConfig(tableEnv, paramsInfo.getConfProp());
        // init global flinkPlanner
        FlinkPlanner.createFlinkPlanner(tableEnv.getFrameworkConfig(), tableEnv.getPlanner(), tableEnv.getTypeFactory());

        return null;
    }

    //创建flink 运行环境
    private static StreamExecutionEnvironment getStreamExeEnv(Properties confProperties, String deployMode) throws Exception {
        StreamExecutionEnvironment env = !ClusterMode.local.name().equals(deployMode) ?
                StreamExecutionEnvironment.getExecutionEnvironment() :
                new MyLocalStreamEnvironment();

        //给env配置运行参数
        StreamEnvConfigManager.streamExecutionEnvironmentConfig(env, confProperties);
        return env;
    }

    /**
     * perjob模式将job依赖的插件包路径存储到cacheFile，在外围将插件包路径传递给jobgraph
     *
     * @param env
     * @param classPathSet
     */
    private static void registerPluginUrlToCachedFile(StreamExecutionEnvironment env, Set<URL> classPathSet) {

    }
}
