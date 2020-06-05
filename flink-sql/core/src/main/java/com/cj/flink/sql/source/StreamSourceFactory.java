package com.cj.flink.sql.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import com.cj.flink.sql.classloader.ClassLoaderManager;
import com.cj.flink.sql.table.AbstractSourceParser;
import com.cj.flink.sql.table.AbstractSourceTableInfo;
import com.cj.flink.sql.util.DtStringUtil;
import com.cj.flink.sql.util.PluginUtil;

public class StreamSourceFactory {
    private static final String CURR_TYPE = "source";

    private static final String DIR_NAME_FORMAT = "%ssource";

    public static AbstractSourceParser getSqlParser(String pluginType, String sqlRootDir) throws Exception {
        String pluginJarPath = PluginUtil.getJarFileDirPath(String.format(DIR_NAME_FORMAT, pluginType), sqlRootDir);

        //eg : db2  返回db
        String typeNoVersion = DtStringUtil.getPluginTypeWithoutVersion(pluginType);

        String className = PluginUtil.getSqlParserClassName(typeNoVersion, CURR_TYPE);


        //../plugin/kafka10source
        return ClassLoaderManager.newInstance(pluginJarPath, (cl) -> {
            Class<?> sourceParser = cl.loadClass(className);
            if(!AbstractSourceParser.class.isAssignableFrom(sourceParser)){
                throw new RuntimeException("class " + sourceParser.getName() + " not subClass of AbsSourceParser");
            }
            return sourceParser.asSubclass(AbstractSourceParser.class).newInstance();
        });


    }

    /**
     * The configuration of the type specified data source
     * @param sourceTableInfo
     * @return
     */
    public static Table getStreamSource(AbstractSourceTableInfo sourceTableInfo, StreamExecutionEnvironment env,
                                        StreamTableEnvironment tableEnv, String sqlRootDir) throws Exception {

        //kafka09
        String sourceTypeStr = sourceTableInfo.getType();
        //kafka
        String typeNoVersion = DtStringUtil.getPluginTypeWithoutVersion(sourceTypeStr);

        String pluginJarPath = PluginUtil.getJarFileDirPath(String.format(DIR_NAME_FORMAT, sourceTypeStr), sqlRootDir);
        //com.cj.flink.sql.source.kafka.KafkaSource
        String className = PluginUtil.getGenerClassName(typeNoVersion, CURR_TYPE);

        return ClassLoaderManager.newInstance(pluginJarPath, (cl) -> {
            Class<?> sourceClass = cl.loadClass(className);
            if(!IStreamSourceGener.class.isAssignableFrom(sourceClass)){
                throw new RuntimeException("class " + sourceClass.getName() + " not subClass of IStreamSourceGener");
            }

            IStreamSourceGener sourceGener = sourceClass.asSubclass(IStreamSourceGener.class).newInstance();
            return (Table) sourceGener.genStreamSource(sourceTableInfo, env, tableEnv);
        });
    }
}
