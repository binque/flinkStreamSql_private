package com.cj.flink.sql.parser;

import com.google.common.collect.Lists;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.calcite.FlinkPlannerImpl;
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

    // 用来标识当前解析节点的上一层节点是否为 insert 节点
    private static Boolean parentIsInsert = false;
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
        FlinkPlannerImpl flinkPlanner = FlinkPlanner.getFlinkPlanner();
        SqlNode sqlNode = flinkPlanner.parse(sql);

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
                parentIsInsert = true;
                parseNode(sqlSource, sqlParseResult);
                break;
            case SELECT:
                SqlSelect sqlSelect = (SqlSelect) sqlNode;
                if (parentIsInsert) {
                    rebuildSelectNode(sqlSelect.getSelectList(), sqlSelect);
                }
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

    /**
     * 将第一层 select 中的 sqlNode 转化为 AsNode，解决字段名冲突问题
     * @param selectList select Node 的 select 字段
     * @param sqlSelect 第一层解析出来的 selectNode
     */
    private static void rebuildSelectNode(SqlNodeList selectList, SqlSelect sqlSelect) {
        SqlNodeList sqlNodes = new SqlNodeList(selectList.getParserPosition());

        for (int index = 0; index < selectList.size(); index++) {
            if (selectList.get(index).getKind().equals(SqlKind.AS)) {
                sqlNodes.add(selectList.get(index));
                continue;
            }
            sqlNodes.add(transformToAsNode(selectList.get(index)));
        }
        sqlSelect.setSelectList(sqlNodes);
    }

    /**
     * 将 sqlNode 转化为 AsNode
     * @param sqlNode 需要转化的 sqlNode
     * @return 重新构造的 AsNode
     */
    public static SqlBasicCall transformToAsNode(SqlNode sqlNode) {
        String asName = "";
        SqlParserPos pos = new SqlParserPos(sqlNode.getParserPosition().getLineNum(),
                sqlNode.getParserPosition().getEndColumnNum());
        if (sqlNode.getKind().equals(SqlKind.IDENTIFIER)) {
            asName = ((SqlIdentifier) sqlNode).names.get(1);
        }
        SqlNode[] operands = new SqlNode[2];
        operands[0] = sqlNode;
        operands[1] = new SqlIdentifier(asName, null, pos);
        return new SqlBasicCall(new SqlAsOperator(), operands, pos);
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
