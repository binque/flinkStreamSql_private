package com.cj.flink.sql.parser;

import com.cj.flink.sql.table.TableInfo;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

public class SqlTree {
    private List<CreateFuncParser.SqlParserResult> functionList = Lists.newArrayList();

    private List<InsertSqlParser.SqlParseResult> execSqlList = Lists.newArrayList();

    private List<CreateTmpTableParser.SqlParserResult> tmpSqlList = Lists.newArrayList();


    private Map<String, CreateTableParser.SqlParserResult> preDealTableMap = Maps.newHashMap();

    private Map<String, TableInfo> tableInfoMap = Maps.newLinkedHashMap();

    private Map<String, CreateTmpTableParser.SqlParserResult> tmpTableMap = Maps.newHashMap();


    public List<CreateFuncParser.SqlParserResult> getFunctionList() {
        return functionList;
    }

    public Map<String, CreateTableParser.SqlParserResult> getPreDealTableMap() {
        return preDealTableMap;
    }

    public Map<String, CreateTmpTableParser.SqlParserResult> getTmpTableMap() {
        return tmpTableMap;
    }

    public List<InsertSqlParser.SqlParseResult> getExecSqlList() {
        return execSqlList;
    }


    public void addFunc(CreateFuncParser.SqlParserResult func){
        functionList.add(func);
    }

    /**
     * 可以理解为处理source，sink 源
     * @param tableName
     * @param table
     */
    public void addPreDealTableInfo(String tableName, CreateTableParser.SqlParserResult table){
        preDealTableMap.put(tableName, table);
    }

    /**
     * 处理中间的临时table
     * @param tableName
     * @param table
     */
    public void addTmplTableInfo(String tableName, CreateTmpTableParser.SqlParserResult table){
        tmpTableMap.put(tableName, table);
    }

    /**
     * 数据转换的逻辑处理
     * @param execSql
     */
    public void addExecSql(InsertSqlParser.SqlParseResult execSql){
        execSqlList.add(execSql);
    }

    /**
     * 创建中间table
     * @param tmpSql
     */
    public void addTmpSql(CreateTmpTableParser.SqlParserResult tmpSql){
        tmpSqlList.add(tmpSql);
    }

    public List<CreateTmpTableParser.SqlParserResult> getTmpSqlList(){
        return tmpSqlList;
    }

    public void addTableInfo(String tableName, TableInfo tableInfo){
        tableInfoMap.put(tableName, tableInfo);
    }

    public Map<String, TableInfo> getTableInfoMap() {
        return tableInfoMap;
    }


}
