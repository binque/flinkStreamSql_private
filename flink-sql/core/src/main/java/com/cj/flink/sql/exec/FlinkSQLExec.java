/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cj.flink.sql.exec;

import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.calcite.FlinkPlannerImpl;
import org.apache.flink.table.plan.logical.LogicalRelNode;
import org.apache.flink.table.plan.schema.TableSinkTable;
import org.apache.flink.table.plan.schema.TableSourceSinkTable;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

import scala.Option;

/**
 * @description:  mapping by name when insert into sink table
 * @author: maqi
 * @create: 2019/08/15 11:09
 */
public class FlinkSQLExec {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkSQLExec.class);

    public static void sqlUpdate(StreamTableEnvironment tableEnv, String stmt, StreamQueryConfig queryConfig) throws Exception {

        FlinkPlannerImpl planner = new FlinkPlannerImpl(tableEnv.getFrameworkConfig(), tableEnv.getPlanner(), tableEnv.getTypeFactory());
        SqlNode insert = planner.parse(stmt);

        if (!(insert instanceof SqlInsert)) {
            throw new TableException(
                    "Unsupported SQL query! sqlUpdate() only accepts SQL statements of type INSERT.");
        }
        SqlNode query = ((SqlInsert) insert).getSource();

        SqlNode validatedQuery = planner.validate(query);

        Table queryResult = new Table(tableEnv, new LogicalRelNode(planner.rel(validatedQuery).rel));
        String targetTableName = ((SqlIdentifier) ((SqlInsert) insert).getTargetTable()).names.get(0);

        Method method = TableEnvironment.class.getDeclaredMethod("getTable", String.class);
        method.setAccessible(true);
        Option sinkTab = (Option)method.invoke(tableEnv, targetTableName);

        if (sinkTab.isEmpty()) {
            throw  new ValidationException("Sink table " + targetTableName + "not found in flink");
        }

        TableSourceSinkTable targetTable = (TableSourceSinkTable) sinkTab.get();
        TableSinkTable tableSinkTable = (TableSinkTable)targetTable.tableSinkTable().get();

        StreamQueryConfig config = null == queryConfig ? tableEnv.queryConfig() : queryConfig;
        String[] sinkFieldNames = tableSinkTable.tableSink().getFieldNames();
        String[] queryFieldNames = queryResult.getSchema().getColumnNames();
        if (sinkFieldNames.length != queryFieldNames.length) {
            throw new ValidationException(
                    "Field name of query result and registered TableSink " + targetTableName + " do not match.\n" +
                            "Query result schema: " + String.join(",", queryFieldNames) + "\n" +
                            "TableSink schema: " + String.join(",", sinkFieldNames));
        }

        Table newTable = null;
        try {
            // sinkFieldNames not in queryResult error
            newTable = queryResult.select(String.join(",", sinkFieldNames));
        } catch (Exception e) {
            throw new ValidationException(
                    "Field name of query result and registered TableSink " + targetTableName + " do not match.\n" +
                            "Query result schema: " + String.join(",", queryResult.getSchema().getColumnNames()) + "\n" +
                            "TableSink schema: " + String.join(",", sinkFieldNames));
        }

        try {
            tableEnv.insertInto(newTable, targetTableName, config);
        } catch (Exception ex) {
            LOG.warn("Field name case of query result and registered TableSink " + targetTableName + "do not match. " + ex.getMessage());
            newTable = queryResult.select(String.join(",", ignoreCase(queryFieldNames, sinkFieldNames)));
            tableEnv.insertInto(newTable, targetTableName, config);
        }
    }

    public static String[] ignoreCase(String[] queryFieldNames, String[] sinkFieldNames) {
        String[] newFieldNames = sinkFieldNames;
        for (int i = 0; i < newFieldNames.length; i++) {
            for (String queryFieldName : queryFieldNames) {
                if (newFieldNames[i].equalsIgnoreCase(queryFieldName)) {
                    newFieldNames[i] = queryFieldName;
                    break;
                }
            }
        }
        return newFieldNames;
    }
}