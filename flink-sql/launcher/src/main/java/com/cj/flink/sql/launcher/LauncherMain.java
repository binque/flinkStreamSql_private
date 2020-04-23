package com.cj.flink.sql.launcher;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.cj.flink.sql.enums.ClusterMode;
import com.cj.flink.sql.option.OptionParser;
import com.cj.flink.sql.option.Options;
import com.cj.flink.sql.util.PluginUtil;
import org.apache.commons.io.Charsets;

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
