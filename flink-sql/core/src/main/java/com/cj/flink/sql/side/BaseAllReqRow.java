/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */



package com.cj.flink.sql.side;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.runtime.types.CRow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.cj.flink.sql.factory.DTThreadFactory;
import org.apache.calcite.sql.JoinType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Reason:
 * Date: 2018/9/18
 * Company: www.dtstack.com
 * @author xuchao
 */

public abstract class BaseAllReqRow extends RichFlatMapFunction<CRow, CRow> implements ISideReqRow {

    private static final Logger LOG = LoggerFactory.getLogger(BaseAllReqRow.class);

    public static final long LOAD_DATA_ERROR_SLEEP_TIME = 5_000L;

    protected BaseSideInfo sideInfo;

    private ScheduledExecutorService es;

    public BaseAllReqRow(BaseSideInfo sideInfo){
        this.sideInfo = sideInfo;

    }

    protected abstract void initCache() throws SQLException;

    protected abstract void reloadCache();

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        initCache();
        LOG.info("----- all cacheRef init end-----");

        //start reload cache thread
        AbstractSideTableInfo sideTableInfo = sideInfo.getSideTableInfo();
        es = new ScheduledThreadPoolExecutor(1,new DTThreadFactory("cache-all-reload"));
        es.scheduleAtFixedRate(() -> reloadCache(), sideTableInfo.getCacheTimeout(), sideTableInfo.getCacheTimeout(), TimeUnit.MILLISECONDS);
    }

    protected void sendOutputRow(CRow value, Object sideInput, Collector<CRow> out) {
        if (sideInput == null && sideInfo.getJoinType() != JoinType.LEFT) {
            return;
        }

        Row row = fillData(value.row(), sideInput);
        out.collect(new CRow(row, value.change()));
    }

    @Override
    public void close() throws Exception {
        if (null != es && !es.isShutdown()) {
            es.shutdown();
        }
    }
}
