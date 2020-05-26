package com.cj.flink.sql.side;

import org.apache.flink.table.api.Table;

import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SideSQLParser {
    private static final Logger LOG = LoggerFactory.getLogger(SideSQLParser.class);

    private Map<String, Table> localTableCache = Maps.newHashMap();
}
