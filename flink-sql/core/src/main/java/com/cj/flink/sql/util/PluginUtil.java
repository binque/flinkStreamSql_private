package com.cj.flink.sql.util;

import com.cj.flink.sql.enums.EPluginLoadMode;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class PluginUtil {

    private static String SP = File.separator;

    private static final String JAR_SUFFIX = ".jar";

    private static final String CLASS_PRE_STR = "com.cj.flink.sql";

    private static ObjectMapper objectMapper = new ObjectMapper();

    /**
     * load source和sink插件
     *
     * @param type                插件类型比如hbase
     * @param suffix              sink
     * @param localSqlPluginPath  本机插件的路径
     * @param remoteSqlPluginPath 远程服务上的路径
     * @param pluginLoadMode      插件加载方式
     * @return
     * @throws Exception
     */
    public static URL buildSourceAndSinkPathByLoadMode(String type, String suffix, String localSqlPluginPath, String remoteSqlPluginPath, String pluginLoadMode) throws Exception {
        if (StringUtils.equalsIgnoreCase(pluginLoadMode, EPluginLoadMode.CLASSPATH.name())) {
            return getRemoteJarFilePath(type, suffix, remoteSqlPluginPath, localSqlPluginPath);
        }
        return getLocalJarFilePath(type, suffix, localSqlPluginPath);
    }

    /**
     * 加载dim插件
     * @param type
     * @param operator
     * @param suffix
     * @param localSqlPluginPath
     * @param remoteSqlPluginPath
     * @param pluginLoadMode
     * @return
     * @throws Exception
     */
    public static URL buildSidePathByLoadMode(String type, String operator, String suffix, String localSqlPluginPath, String remoteSqlPluginPath, String pluginLoadMode) throws Exception {
        if (StringUtils.equalsIgnoreCase(pluginLoadMode, EPluginLoadMode.CLASSPATH.name())) {
            return getRemoteSideJarFilePath(type, operator, suffix, remoteSqlPluginPath, localSqlPluginPath);
        }
        return getLocalSideJarFilePath(type, operator, suffix, localSqlPluginPath);
    }

    /**
     * hbase-async-side
     * @param pluginType
     * @param sideOperator
     * @param tableType
     * @param remoteSqlRootDir
     * @param localSqlPluginPath
     * @return
     * @throws Exception
     */
    public static URL getRemoteSideJarFilePath(String pluginType, String sideOperator, String tableType, String remoteSqlRootDir, String localSqlPluginPath) throws Exception {
        return buildFinalSideJarFilePath(pluginType, sideOperator, tableType, remoteSqlRootDir, localSqlPluginPath);
    }

    public static URL getLocalSideJarFilePath(String pluginType, String sideOperator,  String tableType, String localSqlPluginPath) throws Exception {
        return buildFinalSideJarFilePath(pluginType, sideOperator, tableType, null, localSqlPluginPath);
    }

    /**
     * hbase-async-side
     * @param pluginType    hbase
     * @param sideOperator  async
     * @param tableType     side
     * @param remoteSqlRootDir
     * @param localSqlPluginPath
     * @return
     * @throws Exception
     */
    public static URL buildFinalSideJarFilePath(String pluginType, String sideOperator, String tableType, String remoteSqlRootDir, String localSqlPluginPath) throws Exception {
        String dirName = pluginType + sideOperator + tableType.toLowerCase();
        String prefix = String.format("%s-%s-%s", pluginType, sideOperator, tableType.toLowerCase());
        String jarPath = localSqlPluginPath + SP + dirName;
        String jarName = getCoreJarFileName(jarPath, prefix);
        String sqlRootDir = remoteSqlRootDir == null ? localSqlPluginPath : remoteSqlRootDir;
        return new URL("file:" + sqlRootDir + SP + dirName + SP + jarName);
    }


    public static URL getRemoteJarFilePath(String pluginType, String tableType, String remoteSqlRootDir, String localSqlPluginPath) throws Exception {
        return buildFinalJarFilePath(pluginType, tableType, remoteSqlRootDir, localSqlPluginPath);
    }

    public static URL getLocalJarFilePath(String pluginType, String tableType, String localSqlPluginPath) throws Exception {
        return buildFinalJarFilePath(pluginType, tableType, null, localSqlPluginPath);
    }

    /**
     * 插件包打出来都是../plugins/hbasesink/hbase-sink.jar
     * @param pluginType    hbase
     * @param tableType     sink
     * @param remoteSqlRootDir
     * @param localSqlPluginPath
     * @return
     * @throws Exception
     */
    public static URL buildFinalJarFilePath(String pluginType, String tableType, String remoteSqlRootDir, String localSqlPluginPath) throws Exception {
        String dirName = pluginType + tableType.toLowerCase();
        String prefix = String.format("%s-%s", pluginType, tableType.toLowerCase());
        String jarPath = localSqlPluginPath + SP + dirName;
        String jarName = getCoreJarFileName(jarPath, prefix);
        String sqlRootDir = remoteSqlRootDir == null ? localSqlPluginPath : remoteSqlRootDir;
        return new URL("file:" + sqlRootDir + SP + dirName + SP + jarName);
    }

    public static String getJarFileDirPath(String type, String sqlRootDir){
        String jarPath = sqlRootDir + SP + type;
        File jarFile = new File(jarPath);

        if(!jarFile.exists()){
            throw new RuntimeException(String.format("path %s not exists!!!", jarPath));
        }

        return jarPath;
    }

    public static String getSideJarFileDirPath(String pluginType, String sideOperator, String tableType, String sqlRootDir) throws MalformedURLException {
        String dirName = sqlRootDir + SP + pluginType + sideOperator + tableType.toLowerCase();
        File jarFile = new File(dirName);

        if(!jarFile.exists()){
            throw new RuntimeException(String.format("path %s not exists!!!", dirName));
        }

        return dirName;
    }

    //com.cj.flink.sql.side.kudu.KuduSide
    public static String getGenerClassName(String pluginTypeName, String type) throws IOException {
        String pluginClassName = upperCaseFirstChar(pluginTypeName) + upperCaseFirstChar(type);
        return CLASS_PRE_STR  + "." + type.toLowerCase() + "." + pluginTypeName + "." + pluginClassName;
    }

    //com.cj.flink.sql.source.kafka.table.KafkaSourceParser
    public static String getSqlParserClassName(String pluginTypeName, String type){

        String pluginClassName = upperCaseFirstChar(pluginTypeName) + upperCaseFirstChar(type) +  "Parser";
        return CLASS_PRE_STR  + "." + type.toLowerCase() + "." +  pluginTypeName + ".table." + pluginClassName;
    }

    //com.cj.flink.sql.side.kudu.table.KuduAllReqRow
    public static String getSqlSideClassName(String pluginTypeName, String type, String operatorType){
        String pluginClassName = upperCaseFirstChar(pluginTypeName) + operatorType + "ReqRow";
        return CLASS_PRE_STR  + "." + type.toLowerCase() + "." +  pluginTypeName + "." + pluginClassName;
    }

    public static String upperCaseFirstChar(String str){
        return str.substring(0, 1).toUpperCase() + str.substring(1);
    }

    public static Map<String,Object> objectToMap(Object obj) throws Exception{
        return objectMapper.readValue(objectMapper.writeValueAsBytes(obj), Map.class);
    }

    public static <T> T jsonStrToObject(String jsonStr, Class<T> clazz) throws JsonParseException, JsonMappingException, JsonGenerationException, IOException{
        return  objectMapper.readValue(jsonStr, clazz);
    }

    public static Properties stringToProperties(String str) throws IOException{
        Properties properties = new Properties();
        properties.load(new ByteArrayInputStream(str.getBytes("UTF-8")));
        return properties;
    }

    public static URL[] getPluginJarUrls(String pluginDir) throws MalformedURLException {
        List<URL> urlList = new ArrayList<>();
        File dirFile = new File(pluginDir);
        if(!dirFile.exists() || !dirFile.isDirectory()){
            throw new RuntimeException("plugin path:" + pluginDir + "is not exist.");
        }

        File[] files = dirFile.listFiles(tmpFile -> tmpFile.isFile() && tmpFile.getName().endsWith(JAR_SUFFIX));
        if(files == null || files.length == 0){
            throw new RuntimeException("plugin path:" + pluginDir + " is null.");
        }

        for(File file : files){
            URL pluginJarURL = file.toURI().toURL();
            urlList.add(pluginJarURL);
        }
        return urlList.toArray(new URL[urlList.size()]);
    }

    public static String getCoreJarFileName (String path, String prefix) throws Exception {
        String coreJarFileName = null;
        File pluginDir = new File(path);
        if (pluginDir.exists() && pluginDir.isDirectory()){
            File[] jarFiles = pluginDir.listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    return name.toLowerCase().startsWith(prefix) && name.toLowerCase().endsWith(".jar");
                }
            });

            if (jarFiles != null && jarFiles.length > 0){
                coreJarFileName = jarFiles[0].getName();
            }
        }

        if (StringUtils.isEmpty(coreJarFileName)){
            throw new Exception("Can not find core jar file in path:" + path);
        }

        return coreJarFileName;
    }
}
