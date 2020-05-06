package com.cj.flink.sql.parser;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.calcite.FlinkPlannerImpl;

import com.cj.flink.sql.util.DtStringUtil;
import com.google.common.collect.Lists;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.junit.Test;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.calcite.sql.SqlKind.IDENTIFIER;

/**
 *
 cross join：左表的每一行数据都会关联上UDTF 产出的每一行数据，如果UDTF不产出任何数据，那么这1行不会输出。
 left join：左表的每一行数据都会关联上UDTF 产出的每一行数据，如果UDTF不产出任何数据，则这1行的UDTF的字段会用null值填充。 left join UDTF 语句后面必须接 on true参数。

 *
 * create view udtf_table as
 * 	select	MyTable.userID,MyTable.eventType,MyTable.productID,date1,time1
 *      from MyTable
 *      LEFT JOIN lateral
 *      table(UDTFOneColumnToMultiColumn(productID)) as T(date1,time1) on true;
 */

public class CreateTmpTableParser implements IParser {

    //select table tableName as select
    private static final String PATTERN_STR = "(?i)create\\s+view\\s+([^\\s]+)\\s+as\\s+select\\s+(.*)";

    //CREATE VIEW abc3(name varchar, info varchar);
    private static final String EMPTY_STR = "(?i)^\\screate\\s+view\\s+(\\S+)\\s*\\((.+)\\)$";

    //有数据的view
    private static final Pattern NONEMPTYVIEW = Pattern.compile(PATTERN_STR);

    //空数据的view
    private static final Pattern EMPTYVIEW = Pattern.compile(EMPTY_STR);

    public static CreateTmpTableParser newInstance(){
        return new CreateTmpTableParser();
    }

    @Override
    public boolean verify(String sql) {
        if (Pattern.compile(EMPTY_STR).matcher(sql).find()){
            return true;
        }
        return NONEMPTYVIEW.matcher(sql).find();
    }

    @Override
    public void parseSql(String sql, SqlTree sqlTree) {
        if (NONEMPTYVIEW.matcher(sql).find()){
            Matcher matcher = NONEMPTYVIEW.matcher(sql);
            String tableName = null;
            String selectSql = null;
            if(matcher.find()) {
                tableName = matcher.group(1);
                selectSql = "select " + matcher.group(2);
            }

            FlinkPlannerImpl flinkPlanner = FlinkPlanner.getFlinkPlanner();

            SqlNode sqlNode = null;

            try {
                sqlNode = flinkPlanner.parse(selectSql);
            } catch (Exception e) {
                throw new RuntimeException("", e);
            }

            CreateTmpTableParser.SqlParserResult sqlParseResult = new CreateTmpTableParser.SqlParserResult();
            parseNode(sqlNode, sqlParseResult);

            //创建的临时view的名称
            sqlParseResult.setTableName(tableName);

            //数据来源的sql执行命令
            String transformSelectSql = DtStringUtil.replaceIgnoreQuota(sqlNode.toString(), "`", "");
            sqlParseResult.setExecSql(transformSelectSql);
            sqlTree.addTmpSql(sqlParseResult);

            /**
             * create view udtf_table as
             * 	select	MyTable.userID,MyTable.eventType,MyTable.productID,date1,time1
             *      from MyTable
             *      LEFT JOIN lateral
             *      table(UDTFOneColumnToMultiColumn(productID)) as T(date1,time1) on true;
             */
            //将udtf_table 和下面的select 信息创建对应关系
            sqlTree.addTmplTableInfo(tableName, sqlParseResult);
        }else {
            if (EMPTYVIEW.matcher(sql).find())
            {
                Matcher matcher = EMPTYVIEW.matcher(sql);
                String tableName = null;
                String fieldsInfoStr = null;
                if (matcher.find()){
                    //表名
                    tableName = matcher.group(1);

                    //表字段
                    fieldsInfoStr = matcher.group(2);
                }
                CreateTmpTableParser.SqlParserResult sqlParseResult = new CreateTmpTableParser.SqlParserResult();
                sqlParseResult.setFieldsInfoStr(fieldsInfoStr);
                sqlParseResult.setTableName(tableName);

                sqlTree.addTmplTableInfo(tableName, sqlParseResult);
            }
        }

    }

    //这里主要是解析出有哪些来源表
    private static void parseNode(SqlNode sqlNode, CreateTmpTableParser.SqlParserResult sqlParseResult){
        SqlKind sqlKind = sqlNode.getKind();
        switch (sqlKind){
            case SELECT:
                SqlSelect sqlSelect = (SqlSelect) sqlNode;
                SqlNode from=sqlSelect.getFrom();
                String str = from.toString();
                SqlNode where=sqlSelect.getWhere();
                SqlNodeList selectList=sqlSelect.getSelectList();
                if (from.getKind() == SqlKind.IDENTIFIER){
                    sqlParseResult.addSourceTable(from.toString());
                }else {
                    parseNode(from, sqlParseResult);
                }
                break;
            case JOIN:
                SqlNode leftNode = ((SqlJoin)sqlNode).getLeft();
                SqlNode rightNode = ((SqlJoin)sqlNode).getRight();
                if (leftNode.getKind() == SqlKind.IDENTIFIER){
                    sqlParseResult.addSourceTable(leftNode.toString());
                }else {
                    parseNode(leftNode, sqlParseResult);
                }
                if(rightNode.getKind() == IDENTIFIER){
                    sqlParseResult.addSourceTable(rightNode.toString());
                }else{
                    parseNode(rightNode, sqlParseResult);
                }
                break;
            case AS:
                //不解析column,所以 as 相关的都是表
                SqlNode identifierNode = ((SqlBasicCall)sqlNode).getOperands()[0];
                ((SqlBasicCall)sqlNode).getOperandList();
                if(identifierNode.getKind() != IDENTIFIER){
                    parseNode(identifierNode, sqlParseResult);
                }else {
                    sqlParseResult.addSourceTable(identifierNode.toString());
                }
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
            default:
                System.out.println("");
                //do nothing
                break;
        }
    }

    @Test
    public void test(){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        FlinkPlanner.createFlinkPlanner(tableEnv.getFrameworkConfig(), tableEnv.getPlanner(), tableEnv.getTypeFactory());
        FlinkPlannerImpl flinkPlanner = FlinkPlanner.getFlinkPlanner();


        String sql = "select MyTable.userID,MyTable.eventType,MyTable.productID,date1,time1 as tt" +
                "       from MyTable" +
                "      LEFT JOIN lateral" +
                "      table(UDTFOneColumnToMultiColumn(productID)) as T(date1,time1) on true where date1 > 1";

        String sql1 = "select * from tableName";

        SqlNode sqlNode = null;
        try {
            sqlNode = flinkPlanner.parse(sql);
            System.out.println(sqlNode);
        } catch (Exception e) {
            throw new RuntimeException("", e);
        }
        CreateTmpTableParser.SqlParserResult sqlParseResult = new CreateTmpTableParser.SqlParserResult();
        parseNode(sqlNode, sqlParseResult);
    }

    public static class SqlParserResult {
        /**
         * 表名
         */
        private String tableName;

        /**
         * 字段信息
         */
        private String fieldsInfoStr;

        /**
         * 执行的sql
         */
        private String execSql;

        private List<String> sourceTableList = Lists.newArrayList();

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public String getExecSql() {
            return execSql;
        }

        public void setExecSql(String execSql) {
            this.execSql = execSql;
        }

        public String getFieldsInfoStr() {
            return fieldsInfoStr;
        }

        public void setFieldsInfoStr(String fieldsInfoStr) {
            this.fieldsInfoStr = fieldsInfoStr;
        }

        public void addSourceTable(String sourceTable) {
            sourceTableList.add(sourceTable);
        }

        public List<String> getSourceTableList() {
            return sourceTableList;
        }
    }
}
