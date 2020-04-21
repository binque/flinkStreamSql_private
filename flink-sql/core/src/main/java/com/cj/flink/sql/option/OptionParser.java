package com.cj.flink.sql.option;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;

import java.lang.reflect.Field;
import java.util.List;

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

    }

    private CommandLine addOptions(String[] args) throws ParseException {
        Class cla = propertions.getClass();
        Field[] fields = cla.getDeclaredFields();
        for (Field field:fields) {
            String name = field.getName();
            OptionRequired optionRequired =field.getAnnotation(OptionRequired.class);
            if (optionRequired != null){
                options.addOption(name, optionRequired.hasArg(),optionRequired.description());
            }
        }

        CommandLine cl = parser.parse(options, args);
        return cl;
    }


    private void initOptions(CommandLine cl) throws IllegalAccessException {
        Class cla = propertions.getClass();
        Field[] fields = cla.getDeclaredFields();
        for (Field field:fields) {
            String name = field.getName();
            String value = cl.getOptionValue(name);
            OptionRequired optionRequired = field.getAnnotation(OptionRequired.class);
            if (optionRequired != null){
                if(optionRequired.required()&& StringUtils.isBlank(value)){
                    throw new RuntimeException(String.format("parameters of %s is required",name));
                }
            }
            if(StringUtils.isNotBlank(value)){
                field.setAccessible(true);
                field.set(propertions,value);
            }
        }
    }

    public Options getOptions(){
        return propertions;
    }

    public List<String> getProgramExeArgList() throws Exception {
return null;
    }

    public static void main(String[] args) throws Exception {
        args = new String[]{"-sql","sideSql.txt",
                "-name","xctest",
                "-remoteSqlPluginPath", "/opt/dtstack/150_flinkplugin/sqlplugin",
                "-localSqlPluginPath","D:\\gitspace\\flinkStreamSQL\\plugins",
                "-addjar", "[\"udf.jar\"]",
                "-mode", "yarn",
                "-flinkconf", "D:\\flink_home\\kudu150etc",
                "-yarnconf","D:\\hadoop\\etc\\hadoopkudu",
                "-confProp", "{\"time.characteristic\":\"EventTime\",\"sql.checkpoint.interval\":10000}",
                "-yarnSessionConf", "{\"yid\":\"application_1564971615273_38182\"}"};
        OptionParser optionParser = new OptionParser(args);
        optionParser.addOptions(args);
    }

}
