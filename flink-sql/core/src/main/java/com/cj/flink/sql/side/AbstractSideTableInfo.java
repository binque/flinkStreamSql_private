package com.cj.flink.sql.side;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;

import com.cj.flink.sql.table.AbstractTableInfo;
import com.google.common.collect.Lists;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

public abstract class AbstractSideTableInfo extends AbstractTableInfo implements Serializable {
    public static final String TARGET_SUFFIX = "Side";

    public static final String CACHE_KEY = "cache";

    public static final String CACHE_SIZE_KEY = "cacheSize";

    public static final String CACHE_TTLMS_KEY = "cacheTTLMs";

    public static final String PARTITIONED_JOIN_KEY = "partitionedJoin";

    public static final String CACHE_MODE_KEY = "cacheMode";

    public static final String ASYNC_CAP_KEY = "asyncCapacity";

    public static final String ASYNC_TIMEOUT_KEY = "asyncTimeout";

    public static final String ASYNC_FAIL_MAX_NUM_KEY = "asyncFailMaxNum";

    public static final String CONNECT_RETRY_MAX_NUM_KEY = "connectRetryMaxNum";

    public static final String ASYNC_REQ_POOL_KEY = "asyncPoolSize";

    private String cacheType = "none";

    private int cacheSize = 10000;

    private long cacheTimeout = 60 * 1000L;

    private int  asyncCapacity=100;

    private int  asyncTimeout=10000;

    /**
     *  async operator req outside conn pool size, egg rdb conn pool size
     */
    private int asyncPoolSize = 0;

    private boolean partitionedJoin = false;

    private String cacheMode="ordered";

    private Long asyncFailMaxNum;

    private Integer connectRetryMaxNum;

    private List<PredicateInfo> predicateInfoes = Lists.newArrayList();

    public RowTypeInfo getRowTypeInfo(){
        Class[] fieldClass = getFieldClasses();
        TypeInformation<?>[] types = new TypeInformation[fieldClass.length];
        String[] fieldNames = getFields();
        for(int i=0; i<fieldClass.length; i++){
            types[i] = TypeInformation.of(fieldClass[i]);
        }

        return new RowTypeInfo(types, fieldNames);
    }

    public String getCacheType() {
        return cacheType;
    }

    public void setCacheType(String cacheType) {
        this.cacheType = cacheType;
    }

    public int getCacheSize() {
        return cacheSize;
    }

    public void setCacheSize(int cacheSize) {
        this.cacheSize = cacheSize;
    }

    public long getCacheTimeout() {
        return cacheTimeout;
    }

    public void setCacheTimeout(long cacheTimeout) {
        this.cacheTimeout = cacheTimeout;
    }

    public boolean isPartitionedJoin() {
        return partitionedJoin;
    }

    public void setPartitionedJoin(boolean partitionedJoin) {
        this.partitionedJoin = partitionedJoin;
    }

    public String getCacheMode() {
        return cacheMode;
    }

    public void setCacheMode(String cacheMode) {
        this.cacheMode = cacheMode;
    }

    public int getAsyncCapacity() {
        return asyncCapacity;
    }

    public void setAsyncCapacity(int asyncCapacity) {
        this.asyncCapacity = asyncCapacity;
    }

    public int getAsyncTimeout() {
        return asyncTimeout;
    }

    public void setAsyncTimeout(int asyncTimeout) {
        this.asyncTimeout = asyncTimeout;
    }

    public void setPredicateInfoes(List<PredicateInfo> predicateInfoes) {
        this.predicateInfoes = predicateInfoes;
    }

    public List<PredicateInfo> getPredicateInfoes() {
        return predicateInfoes;
    }

    public Long getAsyncFailMaxNum(Long defaultValue) {
        return Objects.isNull(asyncFailMaxNum) ? defaultValue : asyncFailMaxNum;
    }

    public void setAsyncFailMaxNum(Long asyncFailMaxNum) {
        this.asyncFailMaxNum = asyncFailMaxNum;
    }


    public int getAsyncPoolSize() {
        return asyncPoolSize;
    }

    public void setAsyncPoolSize(int asyncPoolSize) {
        this.asyncPoolSize = asyncPoolSize;
    }


    public Integer getConnectRetryMaxNum(Integer defaultValue) {
        return Objects.isNull(connectRetryMaxNum) ? defaultValue : connectRetryMaxNum;
    }

    public void setConnectRetryMaxNum(Integer connectRetryMaxNum) {
        this.connectRetryMaxNum = connectRetryMaxNum;
    }
    @Override
    public String toString() {
        return "Cache Info{" +
                "cacheType='" + cacheType + '\'' +
                ", cacheSize=" + cacheSize +
                ", cacheTimeout=" + cacheTimeout +
                ", asyncCapacity=" + asyncCapacity +
                ", asyncTimeout=" + asyncTimeout +
                ", asyncPoolSize=" + asyncPoolSize +
                ", asyncFailMaxNum=" + asyncFailMaxNum +
                ", partitionedJoin=" + partitionedJoin +
                ", cacheMode='" + cacheMode + '\'' +
                '}';
    }


}
