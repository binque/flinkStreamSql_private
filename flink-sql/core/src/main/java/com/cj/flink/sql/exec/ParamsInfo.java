package com.cj.flink.sql.exec;

import lombok.Data;

import java.net.URL;
import java.util.List;
import java.util.Properties;

@Data
public class ParamsInfo {
    private String sql;
    private String name;
    private List<URL> jarUrlList;
    private String localSqlPluginPath;
    private String remoteSqlPluginPath;
    private String pluginLoadMode;
    private String deployMode;
    private Properties confProp;

    public ParamsInfo(String sql, String name, List<URL> jarUrlList, String localSqlPluginPath,
                      String remoteSqlPluginPath, String pluginLoadMode, String deployMode, Properties confProp) {
        this.sql = sql;
        this.name = name;
        this.jarUrlList = jarUrlList;
        this.localSqlPluginPath = localSqlPluginPath;
        this.remoteSqlPluginPath = remoteSqlPluginPath;
        this.pluginLoadMode = pluginLoadMode;
        this.deployMode = deployMode;
        this.confProp = confProp;
    }

    @Override
    public String toString() {
        return "ParamsInfo{" +
                "sql='" + sql + '\'' +
                ", name='" + name + '\'' +
                ", jarUrlList=" + convertJarUrlListToString(jarUrlList) +
                ", localSqlPluginPath='" + localSqlPluginPath + '\'' +
                ", remoteSqlPluginPath='" + remoteSqlPluginPath + '\'' +
                ", pluginLoadMode='" + pluginLoadMode + '\'' +
                ", deployMode='" + deployMode + '\'' +
                ", confProp=" + confProp +
                '}';
    }

    public String convertJarUrlListToString(List<URL> jarUrlList) {
        return jarUrlList.stream().map(URL::toString).reduce((pre, last) -> pre + last).orElse("");
    }

    public static ParamsInfo.Builder builder() {
        return new ParamsInfo.Builder();
    }
    public static class Builder {

        private String sql;
        private String name;
        private List<URL> jarUrlList;
        private String localSqlPluginPath;
        private String remoteSqlPluginPath;
        private String pluginLoadMode;
        private String deployMode;
        private String logLevel;
        private Properties confProp;


        public ParamsInfo.Builder setSql(String sql) {
            this.sql = sql;
            return this;
        }

        public ParamsInfo.Builder setName(String name) {
            this.name = name;
            return this;
        }

        public ParamsInfo.Builder setJarUrlList(List<URL> jarUrlList) {
            this.jarUrlList = jarUrlList;
            return this;
        }

        public ParamsInfo.Builder setLocalSqlPluginPath(String localSqlPluginPath) {
            this.localSqlPluginPath = localSqlPluginPath;
            return this;
        }

        public ParamsInfo.Builder setRemoteSqlPluginPath(String remoteSqlPluginPath) {
            this.remoteSqlPluginPath = remoteSqlPluginPath;
            return this;
        }

        public ParamsInfo.Builder setPluginLoadMode(String pluginLoadMode) {
            this.pluginLoadMode = pluginLoadMode;
            return this;
        }

        public ParamsInfo.Builder setDeployMode(String deployMode) {
            this.deployMode = deployMode;
            return this;
        }


        public ParamsInfo.Builder setConfProp(Properties confProp) {
            this.confProp = confProp;
            return this;
        }

        public ParamsInfo build() {
            return new ParamsInfo(sql, name, jarUrlList, localSqlPluginPath,
                    remoteSqlPluginPath, pluginLoadMode, deployMode, confProp);
        }
    }
}
