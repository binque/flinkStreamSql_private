package com.cj.flink.sql.table;

import com.cj.flink.sql.enums.ECacheType;
import com.cj.flink.sql.side.AbstractSideTableInfo;
import com.cj.flink.sql.util.MathUtil;

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
        if(props.containsKey(AbstractSideTableInfo.CACHE_KEY.toLowerCase())){
            String cacheType = MathUtil.getString(props.get(AbstractSideTableInfo.CACHE_KEY.toLowerCase()));
            if(cacheType == null){
                return;
            }

            if(!ECacheType.isValid(cacheType)){
                throw new RuntimeException("can't not support cache type :" + cacheType);
            }

            sideTableInfo.setCacheType(cacheType);

            if(props.containsKey(AbstractSideTableInfo.CACHE_SIZE_KEY.toLowerCase())){
                Integer cacheSize = MathUtil.getIntegerVal(props.get(AbstractSideTableInfo.CACHE_SIZE_KEY.toLowerCase()));
                if(cacheSize < 0){
                    throw new RuntimeException("cache size need > 0.");
                }
                sideTableInfo.setCacheSize(cacheSize);
            }

            if(props.containsKey(AbstractSideTableInfo.CACHE_TTLMS_KEY.toLowerCase())){
                Long cacheTTLMS = MathUtil.getLongVal(props.get(AbstractSideTableInfo.CACHE_TTLMS_KEY.toLowerCase()));
                if(cacheTTLMS < 1000){
                    throw new RuntimeException("cache time out need > 1000 ms.");
                }
                sideTableInfo.setCacheTimeout(cacheTTLMS);
            }

            if(props.containsKey(AbstractSideTableInfo.PARTITIONED_JOIN_KEY.toLowerCase())){
                Boolean partitionedJoinKey = MathUtil.getBoolean(props.get(AbstractSideTableInfo.PARTITIONED_JOIN_KEY.toLowerCase()));
                if(partitionedJoinKey){
                    sideTableInfo.setPartitionedJoin(true);
                }
            }

            if(props.containsKey(AbstractSideTableInfo.CACHE_MODE_KEY.toLowerCase())){
                String cachemode = MathUtil.getString(props.get(AbstractSideTableInfo.CACHE_MODE_KEY.toLowerCase()));

                if(!"ordered".equalsIgnoreCase(cachemode) && !"unordered".equalsIgnoreCase(cachemode)){
                    throw new RuntimeException("cachemode must ordered or unordered!");
                }
                sideTableInfo.setCacheMode(cachemode.toLowerCase());
            }

            if(props.containsKey(AbstractSideTableInfo.ASYNC_CAP_KEY.toLowerCase())){
                Integer asyncCap = MathUtil.getIntegerVal(props.get(AbstractSideTableInfo.ASYNC_CAP_KEY.toLowerCase()));
                if(asyncCap < 0){
                    throw new RuntimeException("asyncCapacity size need > 0.");
                }
                sideTableInfo.setAsyncCapacity(asyncCap);
            }

            if(props.containsKey(AbstractSideTableInfo.ASYNC_TIMEOUT_KEY.toLowerCase())){
                Integer asyncTimeout = MathUtil.getIntegerVal(props.get(AbstractSideTableInfo.ASYNC_TIMEOUT_KEY.toLowerCase()));
                if (asyncTimeout<0){
                    throw new RuntimeException("asyncTimeout size need > 0.");
                }
                sideTableInfo.setAsyncTimeout(asyncTimeout);
            }

            if(props.containsKey(AbstractSideTableInfo.ASYNC_FAIL_MAX_NUM_KEY.toLowerCase())){
                Long asyncFailMaxNum = MathUtil.getLongVal(props.get(AbstractSideTableInfo.ASYNC_FAIL_MAX_NUM_KEY.toLowerCase()));
                if (asyncFailMaxNum<0){
                    throw new RuntimeException("asyncFailMaxNum need > 0.");
                }
                sideTableInfo.setAsyncFailMaxNum(asyncFailMaxNum);
            }

            if(props.containsKey(AbstractSideTableInfo.CONNECT_RETRY_MAX_NUM_KEY.toLowerCase())){
                Integer connectRetryMaxNum = MathUtil.getIntegerVal(props.get(AbstractSideTableInfo.CONNECT_RETRY_MAX_NUM_KEY.toLowerCase()));
                if (connectRetryMaxNum > 0){
                    sideTableInfo.setConnectRetryMaxNum(connectRetryMaxNum);
                }
            }
        }
    }
}
