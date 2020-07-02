package com.cj.flink.sql.option;

import com.cj.flink.sql.util.PluginUtil;
import com.google.common.collect.Lists;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.ParseException;
import org.apache.commons.codec.Charsets;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.FileInputStream;
import java.lang.reflect.Field;
import java.net.URLEncoder;
import java.util.List;
import java.util.Map;

/**
 * @author chenjie
 */
public class OptionParser {

    /**
     * sql参数  真正的任务启动前缀
     */
    public static final String OPTION_SQL = "sql";

    private org.apache.commons.cli.Options options = new org.apache.commons.cli.Options();

    private DefaultParser parser = new DefaultParser();

    private Options propertions = new Options();

    public OptionParser(String[] args) throws Exception {
        initOptions(addOptions(args));
    }

    /**
     * ["-sql" ,"test.test","-mode","yarn"]    将这种类型的参数解析成命令端参数
     * @param args
     * @return
     * @throws ParseException
     */
    private CommandLine addOptions(String[] args) throws ParseException {
        Class cla = propertions.getClass();
        Field[] fields = cla.getDeclaredFields();
        for (Field field : fields) {
            String name = field.getName();
            OptionRequired optionRequired = field.getAnnotation(OptionRequired.class);
            if (optionRequired != null) {
                options.addOption(name, optionRequired.hasArg(), optionRequired.description());
            }
        }

        CommandLine cl = parser.parse(options, args);
        return cl;
    }

    /**
     * 将命令端的参数解析成object
     * @param cl
     * @throws IllegalAccessException
     */
    private void initOptions(CommandLine cl) throws IllegalAccessException {
        Class cla = propertions.getClass();
        Field[] fields = cla.getDeclaredFields();
        for (Field field : fields) {
            String name = field.getName();
            String value = cl.getOptionValue(name);
            OptionRequired optionRequired = field.getAnnotation(OptionRequired.class);
            if (optionRequired != null) {
                if (optionRequired.required() && StringUtils.isBlank(value)) {
                    throw new RuntimeException(String.format("parameters of %s is required", name));
                }
            }
            if (StringUtils.isNotBlank(value)) {
                field.setAccessible(true);
                field.set(propertions, value);
            }
        }
    }

    public Options getOptions() {
        return propertions;
    }

    public List<String> getProgramExeArgList() throws Exception {
        Map<String,Object> mapConf = PluginUtil.objectToMap(propertions);
        List<String> args = Lists.newArrayList();
        for(Map.Entry<String, Object> one : mapConf.entrySet()){
            String key = one.getKey();
            Object value = one.getValue();
            if(value == null){
                continue;
            }else if(OPTION_SQL.equalsIgnoreCase(key)){
                File file = new File(value.toString());
                FileInputStream in = new FileInputStream(file);
                byte[] filecontent = new byte[(int) file.length()];
                in.read(filecontent);
                String content = new String(filecontent, Charsets.UTF_8.name());
                value = URLEncoder.encode(content, Charsets.UTF_8.name());
            }
            args.add("-" + key);
            args.add(value.toString());
        }
        return args;
    }

    public static void main(String[] args) throws Exception {
        args = new String[]{"-sql", "sideSql.txt",
                "-name", "xctest",
                "-remoteSqlPluginPath", "/opt/dtstack/150_flinkplugin/sqlplugin",
                "-localSqlPluginPath", "D:\\gitspace\\flinkStreamSQL\\plugins",
                "-addjar", "[\"udf.jar\"]",
                "-mode", "yarn",
                "-flinkconf", "D:\\flink_home\\kudu150etc",
                "-yarnconf", "D:\\hadoop\\etc\\hadoopkudu",
                "-confProp", "{\"time.characteristic\":\"EventTime\",\"sql.checkpoint.interval\":10000}",
                "-yarnSessionConf", "{\"yid\":\"application_1564971615273_38182\"}"};
        OptionParser optionParser = new OptionParser(args);
        optionParser.addOptions(args);
    }

}
