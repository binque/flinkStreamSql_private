package com.cj.flink.sql.side.cache;

import com.cj.flink.sql.side.AbstractSideTableInfo;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.util.concurrent.TimeUnit;

/**
 * CREATE TABLE sideTable(
 *     id INT,
 *     name VARCHAR,
 *     PRIMARY KEY(id) ,
 *     PERIOD FOR SYSTEM_TIME
 *  )WITH(
 *     type ='mysql',
 *     url ='jdbc:mysql://172.16.10.204:3306/mqtest',
 *     userName ='dtstack',
 *     password ='1abc123',
 *     tableName ='yctest_mysql_10',
 *     partitionedJoin ='false',
 *     cache ='LRU',
 *     cacheSize ='10000',
 *     cacheTTLMs ='60000',
 *     asyncPoolSize ='3',
 *     parallelism ='1'
 *  );
 */
public class LRUSideCache extends AbstractSideCache {

    protected transient Cache<String, CacheObj> cache;

    public LRUSideCache(AbstractSideTableInfo sideTableInfo) {
        super(sideTableInfo);
    }

    @Override
    public void initCache() {
        //当前只有LRU
        cache = CacheBuilder.newBuilder()
                .maximumSize(sideTableInfo.getCacheSize())
                .expireAfterWrite(sideTableInfo.getCacheTimeout(), TimeUnit.MILLISECONDS)   //是在指定项在一定时间内没有创建/覆盖时，会移除该key，下次取的时候从loading中取
                .build();
    }

    @Override
    public CacheObj getFromCache(String key) {
        if(cache == null){
            return null;
        }

        return cache.getIfPresent(key);
    }

    @Override
    public void putCache(String key, CacheObj value) {
        if(cache == null){
            return;
        }

        cache.put(key, value);
    }
}
