package com.cj.flink.sql.table;

import com.cj.flink.sql.side.AbstractSideTableInfo;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 维表
 */
public abstract class AbstractSideTableParser extends AbstractTableParser {
    private final static String SIDE_SIGN_KEY = "sideSignKey";

    private final static Pattern SIDE_TABLE_SIGN = Pattern.compile("(?i)^PERIOD\\s+FOR\\s+SYSTEM_TIME$");

    public AbstractSideTableParser() {
        addParserHandler(SIDE_SIGN_KEY, SIDE_TABLE_SIGN, this::dealSideSign);
    }

    private void dealSideSign(Matcher matcher, AbstractTableInfo tableInfo){
        //FIXME SIDE_TABLE_SIGN current just used as a sign for side table; and do nothing
    }

    protected void parseCacheProp(AbstractSideTableInfo sideTableInfo, Map<String, Object> props){

    }
}
