package com.cj.flink.sql.side;

import com.cj.flink.sql.classloader.ClassLoaderManager;
import com.cj.flink.sql.enums.ECacheType;
import com.cj.flink.sql.table.AbstractSideTableParser;
import com.cj.flink.sql.table.AbstractTableParser;
import com.cj.flink.sql.util.PluginUtil;

public class StreamSideFactory {
    private static final String CURR_TYPE = "side";

    public static AbstractTableParser getSqlParser(String pluginType, String sqlRootDir, String cacheType) throws Exception {
        String sideOperator = ECacheType.ALL.name().equalsIgnoreCase(cacheType) ? "all" : "async";
        String pluginJarPath = PluginUtil.getSideJarFileDirPath(pluginType, sideOperator, "side", sqlRootDir);
        String className = PluginUtil.getSqlParserClassName(pluginType, CURR_TYPE);

        return ClassLoaderManager.newInstance(pluginJarPath, (cl) -> {
            Class<?> sideParser = cl.loadClass(className);
            if (!AbstractSideTableParser.class.isAssignableFrom(sideParser)) {
                throw new RuntimeException("class " + sideParser.getName() + " not subClass of AbsSideTableParser");
            }
            return sideParser.asSubclass(AbstractTableParser.class).newInstance();
        });
    }
}
