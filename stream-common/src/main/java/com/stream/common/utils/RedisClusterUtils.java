package com.stream.common.utils;

import redis.clients.jedis.*;
import redis.clients.jedis.params.SetParams;

import java.util.HashSet;

/**
 * @author yongsheng.zhu
 * @date 2023/11/14
 * @time 11:07
 * RedisClusterCon
 */
public class RedisClusterUtils {
    private static JedisCluster jedisCluster;

    static {
        HashSet<HostAndPort> hostAndPortHashSet = new HashSet<>();
        hostAndPortHashSet.add(new HostAndPort("10.150.20.80", 6379));
        hostAndPortHashSet.add(new HostAndPort("10.150.20.80", 6380));
        hostAndPortHashSet.add(new HostAndPort("10.150.20.81", 6379));
        hostAndPortHashSet.add(new HostAndPort("10.150.20.81", 6380));
        hostAndPortHashSet.add(new HostAndPort("10.150.20.82", 6379));
        hostAndPortHashSet.add(new HostAndPort("10.150.20.82", 6380));

        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        //	资源池中的最大连接数
        jedisPoolConfig.setMaxTotal(200);
        //资源池允许的最大空闲连接数
        jedisPoolConfig.setMaxIdle(20);
        //开启JMX监控
        jedisPoolConfig.setJmxEnabled(true);
        //开启空闲资源检测。
        jedisPoolConfig.setTestWhileIdle(true);
        //对所有连接做空闲监测
        jedisPoolConfig.setNumTestsPerEvictionRun(-1);

        jedisCluster = new JedisCluster(hostAndPortHashSet, 30000, 10, jedisPoolConfig);

    }

    public static String setKeyValue(String key, String value, Long ex) {
        SetParams params = new SetParams();
        if (ex > 0) {
            params.ex(ex);
        }
        return jedisCluster.set(key.trim(), value.trim(), params);
    }

    public static String setKeyValue(String key, String value) {
        return jedisCluster.set(key.trim(), value.trim());
    }

    public static String getValueByKey(String key) {
        return jedisCluster.get(key.trim());
    }

    public static Long delKey(String key) {
        return jedisCluster.del(key);
    }

    public static void main(String[] args) {

        ScanParams scanParams = new ScanParams();
        scanParams.match("btc_address:c68ad54509e021fb96ca518a4b56d43e22f4b045656764ec4e549e112d74058c:1");
        scanParams.count(7270);
        ScanResult<String> result = jedisCluster.scan("0", scanParams);
        result.getResult().forEach(key -> {
            jedisCluster.del(key);
            System.err.println(key);
        });
    }


}
