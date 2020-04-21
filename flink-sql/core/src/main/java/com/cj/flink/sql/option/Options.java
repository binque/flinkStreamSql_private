package com.cj.flink.sql.option;

import com.cj.flink.sql.enums.ClusterMode;
import com.cj.flink.sql.enums.EPluginLoadMode;
import lombok.Data;

@Data
public class Options {

    @OptionRequired(description = "Running mode")
    private  String mode = ClusterMode.local.name();

    @OptionRequired(required = true,description = "Job name")
    private  String name;

    @OptionRequired(required = true,description = "Job sql file")
    private  String sql;

    @OptionRequired(description = "Flink configuration directory")
    private  String flinkconf;

    @OptionRequired(description = "Yarn and Hadoop configuration directory")
    private  String yarnconf;

    @OptionRequired(required = true,description = "Sql local plugin root")
    private  String localSqlPluginPath;

    @OptionRequired(required = false,description = "Sql remote plugin root")
    private  String remoteSqlPluginPath ;

    @OptionRequired(description = "sql ext jar,eg udf jar")
    private  String addjar;

    @OptionRequired(description = "sql ref prop,eg specify event time")
    private  String confProp = "{}";

    @OptionRequired(description = "flink jar path for submit of perjob mode")
    private String flinkJarPath;

    @OptionRequired(description = "yarn queue")
    private String queue = "default";

    @OptionRequired(description = "yarn session configuration,such as yid")
    private String yarnSessionConf = "{}";

    @OptionRequired(description = "plugin load mode, by classpath or shipfile")
    private String pluginLoadMode = EPluginLoadMode.CLASSPATH.name();

    @OptionRequired(description = "log level")
    private String logLevel = "info";
}
