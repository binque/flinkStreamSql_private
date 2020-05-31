package com.cj.flink.sql.function;

import org.apache.flink.table.api.TableEnvironment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FunctionManager {
    private static final Logger logger = LoggerFactory.getLogger(FunctionManager.class);

    /**
     * TABLE|SCALA|AGGREGATE
     * 注册UDF到table env
     */
    public static void registerUDF(String type, String classPath, String funcName, TableEnvironment tableEnv, ClassLoader classLoader) {
        if ("SCALA".equalsIgnoreCase(type)) {
            registerScalaUDF(classPath, funcName, tableEnv, classLoader);
        } else if ("TABLE".equalsIgnoreCase(type)) {
            registerTableUDF(classPath, funcName, tableEnv, classLoader);
        } else if ("AGGREGATE".equalsIgnoreCase(type)) {
            registerAggregateUDF(classPath, funcName, tableEnv, classLoader);
        } else {
            throw new RuntimeException("not support of UDF which is not in (TABLE, SCALA, AGGREGATE)");
        }
    }

    /**
     * 注册自定义方法到env上
     * @param classPath
     * @param funcName
     * @param tableEnv
     */
    public static void registerScalaUDF(String classPath, String funcName, TableEnvironment tableEnv, ClassLoader classLoader) {

    }

    /**
     * 注册自定义TABLEFFUNC方法到env上
     *
     * @param classPath
     * @param funcName
     * @param tableEnv
     */
    public static void registerTableUDF(String classPath, String funcName, TableEnvironment tableEnv, ClassLoader classLoader) {

    }

    /**
     * 注册自定义Aggregate FUNC方法到env上
     *
     * @param classPath
     * @param funcName
     * @param tableEnv
     */
    public static void registerAggregateUDF(String classPath, String funcName, TableEnvironment tableEnv, ClassLoader classLoader) {

    }
}
