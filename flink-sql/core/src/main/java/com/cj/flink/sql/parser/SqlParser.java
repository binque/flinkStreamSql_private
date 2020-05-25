package com.cj.flink.sql.parser;

import com.cj.flink.sql.enums.ETableType;
import com.cj.flink.sql.table.AbstractTableInfo;
import com.cj.flink.sql.table.AbstractTableInfoParser;
import com.cj.flink.sql.util.DtStringUtil;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Set;

public class SqlParser {

    //每一个完整sql是以；结束的
    private static final char SQL_DELIMITER = ';';

    private static String LOCAL_SQL_PLUGIN_ROOT;

    private static List<IParser> sqlParserList = Lists.newArrayList(CreateFuncParser.newInstance(),
            CreateTableParser.newInstance(), InsertSqlParser.newInstance(), CreateTmpTableParser.newInstance());

    public static void setLocalSqlPluginRoot(String localSqlPluginRoot){
        LOCAL_SQL_PLUGIN_ROOT = localSqlPluginRoot;
    }

    /**
     * flink support sql syntax
     * CREATE TABLE sls_stream() with ();
     * CREATE (TABLE|SCALA) FUNCTION fcnName WITH com.dtstack.com;
     * insert into tb1 select * from tb2;
     * @param sql
     */

    public static SqlTree parseSql(String sql) throws Exception {
        if (StringUtils.isBlank(sql)){
            throw new RuntimeException("sql is not null");
        }
        if (LOCAL_SQL_PLUGIN_ROOT == null){
            throw new RuntimeException("need to set local sql plugin root");
        }

        //将sql格式化成一行字符串
        sql = sql.replaceAll("--.*", "")
                .replaceAll("\r\n", " ")
                .replaceAll("\n", " ")
                .replaceAll("\t", " ").trim();

        //以；将每个sql语句分割开
        List<String> sqlArr = DtStringUtil.splitIgnoreQuota(sql, SQL_DELIMITER);
        SqlTree sqlTree = new SqlTree();
        AbstractTableInfoParser tableInfoParser = new AbstractTableInfoParser();
        //遍历sql语句
        for (String childSql: sqlArr) {
            if (Strings.isNullOrEmpty(childSql)){
                continue;
            }
            boolean result = false;
            //每一条sql语句都和四种解析器做匹配CreateFuncParser, CreateTableParser, CreateTmpTableParser， InsertSqlParser
            for (IParser sqlParser: sqlParserList) {
                if (!sqlParser.verify(childSql)){
                    continue;
                }

                sqlParser.parseSql(childSql, sqlTree);
                result = true;
                break;
            }
            if (!result){
                throw new RuntimeException(String.format("%s:Syntax does not support,the format of SQL like insert into tb1 select * from tb2.", childSql));
            }
        }

        //解析exec-sql
        if(sqlTree.getExecSqlList().size() == 0){
            throw new RuntimeException("sql no executable statement");
        }

        for (InsertSqlParser.SqlParseResult result: sqlTree.getExecSqlList()) {
            List<String> sourceTableList = result.getSourceTableList();
            List<String> targetTableList = result.getTargetTableList();
            Set<String> tmpTableList = sqlTree.getTmpTableMap().keySet();

            for(String tableName : sourceTableList){

                //temp 中不包括tableName ,那么这个tableName 的来源只能来自source/side
                if (!tmpTableList.contains(tableName)){
                    CreateTableParser.SqlParserResult createTableResult = sqlTree.getPreDealTableMap().get(tableName);
                    if(createTableResult == null){
                        throw new RuntimeException("can't find table " + tableName);
                    }

                    AbstractTableInfo tableInfo = tableInfoParser.parseWithTableType(ETableType.SOURCE.getType(),
                            createTableResult, LOCAL_SQL_PLUGIN_ROOT);
                    sqlTree.addTableInfo(tableName, tableInfo);
                }
            }

            for(String tableName : targetTableList){
                //temp 中不包括tableName ,那么这个tableName 的来源只能来自sink
                if (!tmpTableList.contains(tableName)){
                    CreateTableParser.SqlParserResult createTableResult = sqlTree.getPreDealTableMap().get(tableName);
                    if(createTableResult == null){
                        throw new RuntimeException("can't find table " + tableName);
                    }

                    AbstractTableInfo tableInfo = tableInfoParser.parseWithTableType(ETableType.SINK.getType(),
                            createTableResult, LOCAL_SQL_PLUGIN_ROOT);
                    sqlTree.addTableInfo(tableName, tableInfo);
                }
            }

        }


        for (CreateTmpTableParser.SqlParserResult result : sqlTree.getTmpSqlList()){
            List<String> sourceTableList = result.getSourceTableList();
            for(String tableName : sourceTableList){
                //临时表中的source不是来源子source节点
                if (!sqlTree.getTableInfoMap().keySet().contains(tableName)){
                    CreateTableParser.SqlParserResult createTableResult = sqlTree.getPreDealTableMap().get(tableName);

                    //一般来说只会是null
                    if(createTableResult == null){
                        CreateTmpTableParser.SqlParserResult tmpTableResult = sqlTree.getTmpTableMap().get(tableName);
                        if (tmpTableResult == null){
                            throw new RuntimeException("can't find table " + tableName);
                        }
                    } else {
                        AbstractTableInfo tableInfo = tableInfoParser.parseWithTableType(ETableType.SOURCE.getType(),
                                createTableResult, LOCAL_SQL_PLUGIN_ROOT);
                        sqlTree.addTableInfo(tableName, tableInfo);
                    }
                }
            }
        }

        return sqlTree;
    }

}
