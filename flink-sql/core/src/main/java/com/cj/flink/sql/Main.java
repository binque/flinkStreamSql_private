package com.cj.flink.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.cj.flink.sql.exec.ExecuteProcessHelper;
import com.cj.flink.sql.exec.ParamsInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        ParamsInfo paramsInfo = ExecuteProcessHelper.parseParams(args);
        StreamExecutionEnvironment env = ExecuteProcessHelper.getStreamExecution(paramsInfo);
    }
}
