package com.cj.flink.sql.parser;


import com.cj.flink.sql.util.DtStringUtil;
import com.google.common.collect.Maps;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * CREATE TABLE MyTable (
 * 	 userID VARCHAR ,
 * 	 eventType VARCHAR,
 * 	 productID VARCHAR)
 * WITH (
 * 	type = 'kafka11',
 * 	bootstrapServers = '172.16.8.107:9092',
 * 	zookeeperQuorum = '172.16.8.107:2181/kafka',
 * 	offsetReset = 'latest',
 * 	topic ='mqTest03',
 * 	topicIsPattern = 'false'
 * );
 *
 *
 * group(0)  MyTable
 * group（1）
 *       userID VARCHAR ,
 *  * 	 eventType VARCHAR,
 *  * 	 productID VARCHAR
 *
 *  group(2)
 *  	type = 'kafka11',
 *  * 	bootstrapServers = '172.16.8.107:9092',
 *  * 	zookeeperQuorum = '172.16.8.107:2181/kafka',
 *  * 	offsetReset = 'latest',
 *  * 	topic ='mqTest03',
 *  * 	topicIsPattern = 'false'
 */
public class CreateTableParser implements IParser {

    private static final String PATTERN_STR = "(?i)create\\s+table\\s+(\\S+)\\s*\\((.+)\\)\\s*with\\s*\\((.+)\\)";

    private static final Pattern PATTERN = Pattern.compile(PATTERN_STR);

    private static final Pattern PROP_PATTERN = Pattern.compile("^'\\s*(.+)\\s*'$");

    public static CreateTableParser newInstance(){
        return new CreateTableParser();
    }

    @Override
    public boolean verify(String sql) {
        return PATTERN.matcher(sql).find();
    }

    @Override
    public void parseSql(String sql, SqlTree sqlTree) {
        Matcher matcher = PATTERN.matcher(sql);
        System.out.println(matcher.find());
        if (!matcher.find()){
            //table的名称
            String tableName = matcher.group(1);
            //字段名称
            String fieldsInfoStr = matcher.group(2);
            String propsStr = matcher.group(3);

            //table的连接参数名称
            Map<String, Object> props = parseProp(propsStr);

            SqlParserResult result = new SqlParserResult();
            result.setTableName(tableName);
            result.setFieldsInfoStr(fieldsInfoStr);
            result.setPropMap(props);

            sqlTree.addPreDealTableInfo(tableName, result);

        }
    }

    @Test
    public void test(){
        CreateTableParser createTableParser = new CreateTableParser();

        SqlTree sqlTree = new SqlTree();
        String sql = "CREATE TABLE source_student ( \n" +
                "     id    INT, \n" +
                "     name varchar, \n" +
                "     age  INT, \n" +
                "     sex  varchar \n" +
                "  )WITH(\n" +
                "    type ='kafka',\n" +
                "    groupId='t3_group_lingqu_lingqu',\n" +
                "    bootstrapServers ='172.16.19.171:9092,172.16.19.172:9092,172.16.19.173:9092',\n" +
                "    topic ='t3_lingqu.test'\n" +
                " )";
        createTableParser.parseSql(sql,sqlTree);
    }

    private Map parseProp(String propsStr){
        String[] strs = propsStr.trim().split("'\\s*,");
        Map<String, Object> propMap = Maps.newHashMap();
        for(int i=0; i<strs.length; i++){
            List<String> ss = DtStringUtil.splitIgnoreQuota(strs[i], '=');
            String key = ss.get(0).trim();
            String value = extractValue(ss.get(1).trim());
            propMap.put(key, value);
        }

        return propMap;
    }

    private String extractValue(String value) {
        Matcher matcher = PROP_PATTERN.matcher(value);
        if (matcher.find()) {
            return matcher.group(1);
        }
        throw new RuntimeException("[" + value + "] format is invalid");
    }

    public static class SqlParserResult{

        private String tableName;

        private String fieldsInfoStr;

        private Map<String, Object> propMap;

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public String getFieldsInfoStr() {
            return fieldsInfoStr;
        }

        public void setFieldsInfoStr(String fieldsInfoStr) {
            this.fieldsInfoStr = fieldsInfoStr;
        }

        public Map<String, Object> getPropMap() {
            return propMap;
        }

        public void setPropMap(Map<String, Object> propMap) {
            this.propMap = propMap;
        }
    }
}
