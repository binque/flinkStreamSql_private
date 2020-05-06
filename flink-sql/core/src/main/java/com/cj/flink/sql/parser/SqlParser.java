package com.cj.flink.sql.parser;

import com.cj.flink.sql.table.TableInfoParser;
import com.cj.flink.sql.util.DtStringUtil;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

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

        sql = sql.replaceAll("--.*", "")
                .replaceAll("\r\n", " ")
                .replaceAll("\n", " ")
                .replaceAll("\t", " ").trim();

        List<String> sqlArr = DtStringUtil.splitIgnoreQuota(sql, SQL_DELIMITER);
        SqlTree sqlTree = new SqlTree();
        TableInfoParser tableInfoParser = new TableInfoParser();
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
        return null;
    }

}
