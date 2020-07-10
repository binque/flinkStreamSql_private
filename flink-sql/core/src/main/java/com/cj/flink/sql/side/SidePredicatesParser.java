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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.calcite.FlinkPlannerImpl;

import com.cj.flink.sql.parser.FlinkPlanner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.apache.calcite.sql.SqlKind.IDENTIFIER;
import static org.apache.calcite.sql.SqlKind.LITERAL;
import static org.apache.calcite.sql.SqlKind.OR;

/**
 * 将同级谓词信息填充到维表
 * Date: 2019/12/11
 * Company: www.dtstack.com
 *
 * @author maqi
 */
public class SidePredicatesParser {
    public void fillPredicatesForSideTable(String exeSql, Map<String, AbstractSideTableInfo> sideTableMap) throws SqlParseException {
        FlinkPlannerImpl flinkPlanner = FlinkPlanner.getFlinkPlanner();
        SqlNode sqlNode = flinkPlanner.parse(exeSql);
        parseSql(sqlNode, sideTableMap, Maps.newHashMap());
    }

    @Test
    public void test() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        FlinkPlanner.createFlinkPlanner(tableEnv.getFrameworkConfig(), tableEnv.getPlanner(), tableEnv.getTypeFactory());
        FlinkPlannerImpl flinkPlanner = FlinkPlanner.getFlinkPlanner();
        String exeSql1 = "SELECT d.channel,\n" +
                "        d.info\n" +
                "    FROM\n" +
                "        (      SELECT\n" +
                "            a.*,b.info\n" +
                "        FROM\n" +
                "            MyTable a\n" +
                "        JOIN\n" +
                "            sideTable b\n" +
                "                ON a.channel=b.name\n" +
                "        ) as d";
        String exeSql = "select MyTable.userID,MyTable.eventType,MyTable.productID,date1,time1\n" +
                "       from MyTable\n" +
                "       LEFT JOIN lateral\n" +
                "       table(UDTFOneColumnToMultiColumn(productID)) as T(date1,time1) on true";
        SqlNode sqlNode = flinkPlanner.parse(exeSql);
        Map<String, AbstractSideTableInfo> sideTableMap = Maps.newHashMap();
        parseSql(sqlNode, sideTableMap, Maps.newHashMap());
        System.out.println(sideTableMap);

    }

    /**
     * 将谓词信息填充到维表属性
     *
     * @param sqlNode
     * @param sideTableMap
     * @param tabMapping   谓词属性中别名对应的真实维表名称
     */
    private void parseSql(SqlNode sqlNode, Map<String, AbstractSideTableInfo> sideTableMap, Map<String, String> tabMapping) {
        SqlKind sqlKind = sqlNode.getKind();
        switch (sqlKind) {
            case INSERT:
                SqlNode sqlSource = ((SqlInsert) sqlNode).getSource();
                parseSql(sqlSource, sideTableMap, tabMapping);
                break;
            case SELECT:
                SqlNode fromNode = ((SqlSelect) sqlNode).getFrom();
                SqlNode whereNode = ((SqlSelect) sqlNode).getWhere();

                if (fromNode.getKind() != IDENTIFIER) {
                    parseSql(fromNode, sideTableMap, tabMapping);
                }
                //  带or的不解析
                if (null != whereNode && whereNode.getKind() != OR) {
                    List<PredicateInfo> predicateInfoList = Lists.newArrayList();
                    extractPredicateInfo(whereNode, predicateInfoList);
                    fillToSideTableInfo(sideTableMap, tabMapping, predicateInfoList);
                }
                break;
            case JOIN:
                SqlNode leftNode = ((SqlJoin) sqlNode).getLeft();
                SqlNode rightNode = ((SqlJoin) sqlNode).getRight();
                parseSql(leftNode, sideTableMap, tabMapping);
                parseSql(rightNode, sideTableMap, tabMapping);
                break;
            case AS:
                SqlNode info = ((SqlBasicCall) sqlNode).getOperands()[0];
                SqlNode alias = ((SqlBasicCall) sqlNode).getOperands()[1];
                if (info.getKind() == IDENTIFIER) {
                    tabMapping.put(alias.toString(), info.toString());
                } else {
                    // 为子查询创建一个同级map
                    parseSql(info, sideTableMap, Maps.newHashMap());
                }
                break;
            case UNION:
                SqlNode unionLeft = ((SqlBasicCall) sqlNode).getOperands()[0];
                SqlNode unionRight = ((SqlBasicCall) sqlNode).getOperands()[1];
                parseSql(unionLeft, sideTableMap, tabMapping);
                parseSql(unionRight, sideTableMap, tabMapping);
                break;
            default:
                break;
        }
    }

    private void fillToSideTableInfo(Map<String, AbstractSideTableInfo> sideTableMap, Map<String, String> tabMapping, List<PredicateInfo> predicateInfoList) {
        predicateInfoList.stream().filter(info -> sideTableMap.containsKey(tabMapping.getOrDefault(info.getOwnerTable(), info.getOwnerTable())))
                .map(info -> sideTableMap.get(tabMapping.getOrDefault(info.getOwnerTable(), info.getOwnerTable())).getPredicateInfoes().add(info))
                .count();
    }


    private void extractPredicateInfo(SqlNode whereNode, List<PredicateInfo> predicatesInfoList) {
        SqlKind sqlKind = whereNode.getKind();
        if (sqlKind == SqlKind.AND && ((SqlBasicCall) whereNode).getOperandList().size() == 2) {
            extractPredicateInfo(((SqlBasicCall) whereNode).getOperands()[0], predicatesInfoList);
            extractPredicateInfo(((SqlBasicCall) whereNode).getOperands()[1], predicatesInfoList);
        } else {
            SqlOperator operator = ((SqlBasicCall) whereNode).getOperator();
            String operatorName = operator.getName();
            SqlKind operatorKind = operator.getKind();

            if (operatorKind == SqlKind.IS_NOT_NULL || operatorKind == SqlKind.IS_NULL) {
                fillPredicateInfoToList((SqlBasicCall) whereNode, predicatesInfoList, operatorName, operatorKind, 0, 0);
                return;
            }

            // 跳过函数
            if ((((SqlBasicCall) whereNode).getOperands()[0] instanceof SqlIdentifier)
                    && (((SqlBasicCall) whereNode).getOperands()[1].getKind() == SqlKind.LITERAL)) {
                fillPredicateInfoToList((SqlBasicCall) whereNode, predicatesInfoList, operatorName, operatorKind, 0, 1);
            } else if ((((SqlBasicCall) whereNode).getOperands()[1] instanceof SqlIdentifier)
                    && (((SqlBasicCall) whereNode).getOperands()[0].getKind() == LITERAL)) {
                fillPredicateInfoToList((SqlBasicCall) whereNode, predicatesInfoList, operatorName, operatorKind, 1, 0);
            }
        }
    }

    private void fillPredicateInfoToList(SqlBasicCall whereNode, List<PredicateInfo> predicatesInfoList, String operatorName, SqlKind operatorKind,
                                         int fieldIndex, int conditionIndex) {
        SqlNode sqlNode = whereNode.getOperands()[fieldIndex];
        if (sqlNode.getKind() == SqlKind.IDENTIFIER) {
            SqlIdentifier fieldFullPath = (SqlIdentifier) sqlNode;
            if (fieldFullPath.names.size() == 2) {
                String ownerTable = fieldFullPath.names.get(0);
                String fieldName = fieldFullPath.names.get(1);
                String content = (operatorKind == SqlKind.BETWEEN) ? whereNode.getOperands()[conditionIndex].toString() + " AND " +
                        whereNode.getOperands()[2].toString() : whereNode.getOperands()[conditionIndex].toString();

                if (StringUtils.containsIgnoreCase(content, SqlKind.CASE.toString())) {
                    return;
                }

                PredicateInfo predicateInfo = PredicateInfo.builder().setOperatorName(operatorName).setOperatorKind(operatorKind.toString())
                        .setOwnerTable(ownerTable).setFieldName(fieldName).setCondition(content).build();
                predicatesInfoList.add(predicateInfo);
            }
        }
    }

}
