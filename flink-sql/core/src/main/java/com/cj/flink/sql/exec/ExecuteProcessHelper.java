package com.cj.flink.sql.exec;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.calcite.FlinkPlannerImpl;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

import com.cj.flink.sql.classloader.ClassLoaderManager;
import com.cj.flink.sql.enums.ClusterMode;
import com.cj.flink.sql.enums.ECacheType;
import com.cj.flink.sql.enums.EPluginLoadMode;
import com.cj.flink.sql.environment.MyLocalStreamEnvironment;
import com.cj.flink.sql.environment.StreamEnvConfigManager;
import com.cj.flink.sql.function.FunctionManager;
import com.cj.flink.sql.option.OptionParser;
import com.cj.flink.sql.option.Options;
import com.cj.flink.sql.parser.CreateFuncParser;
import com.cj.flink.sql.parser.CreateTmpTableParser;
import com.cj.flink.sql.parser.FlinkPlanner;
import com.cj.flink.sql.parser.InsertSqlParser;
import com.cj.flink.sql.parser.SqlParser;
import com.cj.flink.sql.parser.SqlTree;
import com.cj.flink.sql.side.AbstractSideTableInfo;
import com.cj.flink.sql.side.SideSqlExec;
import com.cj.flink.sql.sink.StreamSinkFactory;
import com.cj.flink.sql.source.StreamSourceFactory;
import com.cj.flink.sql.table.AbstractSourceTableInfo;
import com.cj.flink.sql.table.AbstractTableInfo;
import com.cj.flink.sql.table.AbstractTargetTableInfo;
import com.cj.flink.sql.util.DtStringUtil;
import com.cj.flink.sql.util.PluginUtil;
import com.cj.flink.sql.watermarker.WaterMarkerAssigner;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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
     *
     * @param paramsInfo
     * @return
     * @throws Exception
     */
    public static StreamExecutionEnvironment getStreamExecution(ParamsInfo paramsInfo) throws Exception {
        //根据不同的运行模式选择不同的执行环境
        StreamExecutionEnvironment env = ExecuteProcessHelper.getStreamExeEnv(paramsInfo.getConfProp(), paramsInfo.getDeployMode());
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        StreamQueryConfig streamQueryConfig = StreamEnvConfigManager.getStreamQueryConfig(tableEnv, paramsInfo.getConfProp());
        // init global flinkPlanner  这里用来解析sql语句的
        FlinkPlanner.createFlinkPlanner(tableEnv.getFrameworkConfig(), tableEnv.getPlanner(), tableEnv.getTypeFactory());

        SqlParser.setLocalSqlPluginRoot(paramsInfo.getLocalSqlPluginPath());
        SqlTree sqlTree = SqlParser.parseSql(paramsInfo.getSql());

        Map<String, AbstractSideTableInfo> sideTableMap = Maps.newHashMap();
        Map<String, Table> registerTableCache = Maps.newHashMap();

        //register udf
        ExecuteProcessHelper.registerUserDefinedFunction(sqlTree, paramsInfo.getJarUrlList(), tableEnv);
//register table schema
        /**
         * sqlTree : 包含所有表信息字段的sqlTree
         * env
         * tableEnv
         * paramsInfo.getLocalSqlPluginPath()  : 查件路径
         * paramsInfo.getRemoteSqlPluginPath() : 一般是将插件包放在hdfs上
         */
        Set<URL> classPathSets = ExecuteProcessHelper.registerTable(sqlTree, env, tableEnv, paramsInfo.getLocalSqlPluginPath(),
                paramsInfo.getRemoteSqlPluginPath(), paramsInfo.getPluginLoadMode(), sideTableMap, registerTableCache);

        ExecuteProcessHelper.registerPluginUrlToCachedFile(env, classPathSets);

        ExecuteProcessHelper.sqlTranslation(paramsInfo.getLocalSqlPluginPath(), tableEnv, sqlTree, sideTableMap, registerTableCache, streamQueryConfig);

        if (env instanceof MyLocalStreamEnvironment) {
            ((MyLocalStreamEnvironment) env).setClasspaths(ClassLoaderManager.getClassPath());
        }
        return env;
    }

    private static void sqlTranslation(String localSqlPluginPath,
                                       StreamTableEnvironment tableEnv,
                                       SqlTree sqlTree,
                                       Map<String, AbstractSideTableInfo> sideTableMap,
                                       Map<String, Table> registerTableCache,
                                       StreamQueryConfig queryConfig) throws Exception {
        SideSqlExec sideSqlExec = new SideSqlExec();
        sideSqlExec.setLocalSqlPluginPath(localSqlPluginPath);
        for (CreateTmpTableParser.SqlParserResult result : sqlTree.getTmpSqlList()) {
            sideSqlExec.exec(result.getExecSql(), sideTableMap, tableEnv, registerTableCache, queryConfig, result);
        }

        for (InsertSqlParser.SqlParseResult result : sqlTree.getExecSqlList()) {
            if (LOG.isInfoEnabled()) {
                LOG.info("exe-sql:\n" + result.getExecSql());
            }
            boolean isSide = false;
            for (String tableName : result.getTargetTableList()) {
                //中间表，create 出来的表
                if (sqlTree.getTmpTableMap().containsKey(tableName)) {
                    CreateTmpTableParser.SqlParserResult tmp = sqlTree.getTmpTableMap().get(tableName);

                    //这里因为某些字段名两侧有``
                    String realSql = DtStringUtil.replaceIgnoreQuota(result.getExecSql(), "`", "");

                    FlinkPlannerImpl flinkPlanner = FlinkPlanner.getFlinkPlanner();
                    SqlNode sqlNode = flinkPlanner.parse(realSql);
                    String tmpSql = ((SqlInsert) sqlNode).getSource().toString();
                    tmp.setExecSql(tmpSql);
                    sideSqlExec.exec(tmp.getExecSql(), sideTableMap, tableEnv, registerTableCache, queryConfig, tmp);
                } else {
                    for (String sourceTable : result.getSourceTableList()) {
                        if (sideTableMap.containsKey(sourceTable)) {
                            isSide = true;
                            break;
                        }
                    }
                    if (isSide) {
                        //sql-dimensional table contains the dimension table of execution
                        sideSqlExec.exec(result.getExecSql(), sideTableMap, tableEnv, registerTableCache, queryConfig, null);
                    } else {
                        LOG.info("----------exec sql without dimension join-----------");
                        LOG.info("----------real sql exec is--------------------------\n{}", result.getExecSql());
                        FlinkSQLExec.sqlUpdate(tableEnv, result.getExecSql(), queryConfig);
                        if (LOG.isInfoEnabled()) {
                            LOG.info("exec sql: " + result.getExecSql());
                        }
                    }
                }
            }
        }
    }

    private static void registerUserDefinedFunction(SqlTree sqlTree, List<URL> jarUrlList, TableEnvironment tableEnv)
            throws IllegalAccessException, InvocationTargetException {
        // udf和tableEnv须由同一个类加载器加载
        ClassLoader levelClassLoader = tableEnv.getClass().getClassLoader();
        URLClassLoader classLoader = null;

        //找到create function 中的udf函数
        List<CreateFuncParser.SqlParserResult> funcList = sqlTree.getFunctionList();
        for (CreateFuncParser.SqlParserResult funcInfo : funcList) {
            //classloader
            if (classLoader == null) {
                classLoader = ClassLoaderManager.loadExtraJar(jarUrlList, (URLClassLoader) levelClassLoader);
            }

            //TABLE|SCALA|AGGREGATE
            FunctionManager.registerUDF(funcInfo.getType(), funcInfo.getClassName(), funcInfo.getName(), tableEnv, classLoader);
        }
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
     * 向Flink注册源表和结果表，返回执行时插件包的全路径
     *
     * @param sqlTree
     * @param env
     * @param tableEnv
     * @param localSqlPluginPath
     * @param remoteSqlPluginPath
     * @param pluginLoadMode      插件加载模式 classpath or shipfile
     * @param sideTableMap
     * @param registerTableCache
     * @return
     * @throws Exception
     */
    public static Set<URL> registerTable(SqlTree sqlTree, StreamExecutionEnvironment env, StreamTableEnvironment tableEnv, String localSqlPluginPath,
                                         String remoteSqlPluginPath, String pluginLoadMode, Map<String, AbstractSideTableInfo> sideTableMap, Map<String, Table> registerTableCache) throws Exception {
        Set<URL> pluginClassPathSets = Sets.newHashSet();
        WaterMarkerAssigner waterMarkerAssigner = new WaterMarkerAssigner();
        for (AbstractTableInfo tableInfo : sqlTree.getTableInfoMap().values()) {
            if (tableInfo instanceof AbstractSourceTableInfo) {
                AbstractSourceTableInfo sourceTableInfo = (AbstractSourceTableInfo) tableInfo;
                Table table = StreamSourceFactory.getStreamSource(sourceTableInfo, env, tableEnv, localSqlPluginPath);

                //注册表名
                //这里并不是真正的source表，用来形成watermark 和注册 事件时间字段或系统时间字段
                tableEnv.registerTable(sourceTableInfo.getAdaptName(), table);

                String adaptSql = sourceTableInfo.getAdaptSelectSql();
                Table adaptTable = adaptSql == null ? table : tableEnv.sqlQuery(adaptSql);
                RowTypeInfo typeInfo = new RowTypeInfo(adaptTable.getSchema().getFieldTypes(), adaptTable.getSchema().getFieldNames());
                DataStream adaptStream = tableEnv.toRetractStream(adaptTable, typeInfo)
                        .map((Tuple2<Boolean, Row> f0) -> {
                            return f0.f1;
                        })
                        .returns(typeInfo);
                String fields = String.join(",", typeInfo.getFieldNames());

                if (waterMarkerAssigner.checkNeedAssignWaterMarker(sourceTableInfo)) {
                    adaptStream = waterMarkerAssigner.assignWaterMarker(adaptStream, typeInfo, sourceTableInfo);
                    String eventTimeField = sourceTableInfo.getEventTimeField();
                    boolean hasEventTimeField = false;
                    if (!Strings.isNullOrEmpty(eventTimeField)) {
                        String[] fieldArray = fields.split(",");
                        for (int i = 0; i < fieldArray.length; i++) {
                            if (fieldArray[i].equals(eventTimeField)) {
                                fieldArray[i] = eventTimeField + ".ROWTIME";
                                hasEventTimeField = true;
                                break;
                            }
                        }
                        if (hasEventTimeField) {
                            fields = String.join(",", fieldArray);
                        } else {
                            fields += ",ROWTIME.ROWTIME";
                        }
                    }
                } else {
                    fields += ",PROCTIME.PROCTIME";
                }

                Table regTable = tableEnv.fromDataStream(adaptStream, fields);
                tableEnv.registerTable(tableInfo.getName(), regTable);

                if (LOG.isInfoEnabled()) {
                    LOG.info("registe table {} success.", tableInfo.getName());
                }
                registerTableCache.put(tableInfo.getName(), regTable);
                URL sourceTablePathUrl = PluginUtil.buildSourceAndSinkPathByLoadMode(tableInfo.getType(), AbstractSourceTableInfo.SOURCE_SUFFIX, localSqlPluginPath, remoteSqlPluginPath, pluginLoadMode);
                pluginClassPathSets.add(sourceTablePathUrl);

            }else if (tableInfo instanceof AbstractTargetTableInfo) {
                //localSqlPluginPath 用来找到sink Parser 解析的
                TableSink tableSink = StreamSinkFactory.getTableSink((AbstractTargetTableInfo) tableInfo, localSqlPluginPath);
                TypeInformation[] flinkTypes = FunctionManager.transformTypes(tableInfo.getFieldClasses());
                tableEnv.registerTableSink(tableInfo.getName(), tableInfo.getFields(), flinkTypes, tableSink);

                URL sinkTablePathUrl = PluginUtil.buildSourceAndSinkPathByLoadMode(tableInfo.getType(), AbstractTargetTableInfo.TARGET_SUFFIX, localSqlPluginPath, remoteSqlPluginPath, pluginLoadMode);
                pluginClassPathSets.add(sinkTablePathUrl);
            } else if (tableInfo instanceof AbstractSideTableInfo) {
                String sideOperator = ECacheType.ALL.name().equals(((AbstractSideTableInfo) tableInfo).getCacheType()) ? "all" : "async";
                sideTableMap.put(tableInfo.getName(), (AbstractSideTableInfo) tableInfo);

                URL sideTablePathUrl = PluginUtil.buildSidePathByLoadMode(tableInfo.getType(), sideOperator, AbstractSideTableInfo.TARGET_SUFFIX, localSqlPluginPath, remoteSqlPluginPath, pluginLoadMode);
                pluginClassPathSets.add(sideTablePathUrl);
            }else {
                throw new RuntimeException("not support table type:" + tableInfo.getType());
            }
        }

        return pluginClassPathSets;
    }

    /**
     * perjob模式将job依赖的插件包路径存储到cacheFile，在外围将插件包路径传递给jobgraph
     *
     * @param env
     * @param classPathSet
     */
    private static void registerPluginUrlToCachedFile(StreamExecutionEnvironment env, Set<URL> classPathSet) {
        int i = 0;
        for (URL url : classPathSet) {
            String classFileName = String.format(CLASS_FILE_NAME_FMT, i);
            env.registerCachedFile(url.getPath(), classFileName, true);
            i++;
        }
    }
}
