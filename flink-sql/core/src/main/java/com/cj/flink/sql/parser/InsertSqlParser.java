package com.cj.flink.sql.parser;

import com.google.common.collect.Lists;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlMatchRecognize;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.util.List;

import static org.apache.calcite.sql.SqlKind.IDENTIFIER;

/**
 * insert
 * into
 *     MyResult
 *     select
 *         d.channel,
 *         d.info
 *     from
 *         abc3 as d;
 */

public class InsertSqlParser implements IParser {
    @Override
    public boolean verify(String sql) {
        return StringUtils.isNotBlank(sql) && sql.trim().toLowerCase().startsWith("insert");
    }

    public static InsertSqlParser newInstance(){
        InsertSqlParser parser = new InsertSqlParser();
        return parser;
    }

    @Override
    public void parseSql(String sql, SqlTree sqlTree) {
        SqlParser.Config config = SqlParser
                                        .configBuilder()
                                        .setLex(Lex.MYSQL)//使用mysql 语法
                                        .build();
        SqlParser sqlParser = SqlParser.create(sql,config);
        SqlNode sqlNode = null;
        try {
            sqlNode = sqlParser.parseStmt();
        } catch (SqlParseException e) {
            throw new RuntimeException("", e);
        }
        SqlParseResult sqlParseResult = new SqlParseResult();
        parseNode(sqlNode, sqlParseResult);
        sqlParseResult.setExecSql(sqlNode.toString());
        sqlTree.addExecSql(sqlParseResult);
    }

    private static void parseNode(SqlNode sqlNode, SqlParseResult sqlParseResult){
        SqlKind sqlKind = sqlNode.getKind();
        switch (sqlKind){
            case INSERT:
                SqlNode sqlTarget = ((SqlInsert)sqlNode).getTargetTable();
                SqlNode sqlSource = ((SqlInsert)sqlNode).getSource();
                sqlParseResult.addTargetTable(sqlTarget.toString());
                parseNode(sqlSource, sqlParseResult);
                break;
            case SELECT:
                SqlNode sqlFrom = ((SqlSelect)sqlNode).getFrom();
                if(sqlFrom.getKind() == IDENTIFIER){
                    sqlParseResult.addSourceTable(sqlFrom.toString());
                }else{
                    parseNode(sqlFrom, sqlParseResult);
                }
                break;
            case AS:
                //不解析column,所以 as 相关的都是表
                SqlNode identifierNode = ((SqlBasicCall)sqlNode).getOperands()[0];
                if(identifierNode.getKind() != IDENTIFIER){
                    parseNode(identifierNode, sqlParseResult);
                }else {
                    sqlParseResult.addSourceTable(identifierNode.toString());
                }
                break;
            case JOIN:
                SqlNode leftNode = ((SqlJoin)sqlNode).getLeft();
                SqlNode rightNode = ((SqlJoin)sqlNode).getRight();

                if(leftNode.getKind() == IDENTIFIER){
                    sqlParseResult.addSourceTable(leftNode.toString());
                }else{
                    parseNode(leftNode, sqlParseResult);
                }

                if(rightNode.getKind() == IDENTIFIER){
                    sqlParseResult.addSourceTable(rightNode.toString());
                }else{
                    parseNode(rightNode, sqlParseResult);
                }
                break;
            case MATCH_RECOGNIZE:
                SqlMatchRecognize node = (SqlMatchRecognize) sqlNode;
                sqlParseResult.addSourceTable(node.getTableRef().toString());
                break;
            case UNION:
                SqlNode unionLeft = ((SqlBasicCall)sqlNode).getOperands()[0];
                SqlNode unionRight = ((SqlBasicCall)sqlNode).getOperands()[1];
                if(unionLeft.getKind() == IDENTIFIER){
                    sqlParseResult.addSourceTable(unionLeft.toString());
                }else{
                    parseNode(unionLeft, sqlParseResult);
                }
                if(unionRight.getKind() == IDENTIFIER){
                    sqlParseResult.addSourceTable(unionRight.toString());
                }else{
                    parseNode(unionRight, sqlParseResult);
                }
                break;
            case ORDER_BY:
                SqlOrderBy sqlOrderBy  = (SqlOrderBy) sqlNode;
                parseNode(sqlOrderBy.query, sqlParseResult);
                break;
            default:
                //do nothing
                break;
        }
    }

    @Test
    public void test(){
        String sql = "insert into" +
                "       MyResult" +
                        "    select" +
                        "        d.channel," +
                        "        d.info" +
                        "    from" +
                        "        abc3 as d order by d.info";
        SqlParser.Config config = SqlParser
                .configBuilder()
                .setLex(Lex.MYSQL)//使用mysql 语法
                .build();
        SqlParser sqlParser = SqlParser.create(sql,config);
        SqlNode sqlNode = null;
        try {
            sqlNode = sqlParser.parseStmt();
        } catch (SqlParseException e) {
            throw new RuntimeException("", e);
        }
        SqlParseResult sqlParseResult = new SqlParseResult();
        parseNode(sqlNode, sqlParseResult);
    }

    public static class SqlParseResult {

        private List<String> sourceTableList = Lists.newArrayList();

        private List<String> targetTableList = Lists.newArrayList();

        private String execSql;

        public void addSourceTable(String sourceTable){
            sourceTableList.add(sourceTable);
        }

        public void addTargetTable(String targetTable){
            targetTableList.add(targetTable);
        }

        public List<String> getSourceTableList() {
            return sourceTableList;
        }

        public List<String> getTargetTableList() {
            return targetTableList;
        }

        public String getExecSql() {
            return execSql;
        }

        public void setExecSql(String execSql) {
            this.execSql = execSql;
        }
    }
}
