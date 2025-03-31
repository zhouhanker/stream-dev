package com.stream.common.utils;



import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisException;

import java.net.URI;
import java.util.*;

/**
 * @Package com.stream.common.utils.RedisUtils.java
 * @Author zhou.han
 * @Date 2025/3/29 23:09
 * @description: Lua check redis words
 */

public class RedisLuaUtils {
    private static final Logger logger = LoggerFactory.getLogger(RedisLuaUtils.class);
    private static final String REDIS_HOST = ConfigUtils.getString("redis.host");
    private static final int REDIS_PORT = ConfigUtils.getInt("redis.port");
    private static final String REDIS_USER = ConfigUtils.getString("redis.user");
    private static final String REDIS_PASSWORD = ConfigUtils.getString("redis.pwd");
    private static final int REDIS_DB = ConfigUtils.getInt("redis.blacklist.db");
    private static final String SET_KEY = "sensitive_words";

    // Lua脚本（支持批量/单条查询）
    private static final String LUA_SCRIPT =
            "local results = {} " +
                    "for i, key in ipairs(ARGV) do " +
                    "    results[i] = redis.call('SISMEMBER', KEYS[1], key) " +
                    "end " +
                    "return results";

    private static volatile String luaScriptSha;
    private static JedisPool jedisPool = null;

    static {
        initializeRedisPool();
    }

    private static void initializeRedisPool() {
        try {
            JedisPoolConfig poolConfig = new JedisPoolConfig();
            poolConfig.setMaxTotal(200);
            poolConfig.setMaxIdle(50);
            poolConfig.setMinIdle(10);
            poolConfig.setTestOnBorrow(true);

            String uri = String.format("redis://%s:%s@%s:%d/%d",
                    REDIS_USER,
                    REDIS_PASSWORD,
                    REDIS_HOST,
                    REDIS_PORT,
                    REDIS_DB);

            jedisPool = new JedisPool(
                    poolConfig,
                    URI.create(uri),
                    Protocol.DEFAULT_TIMEOUT
            );

            preloadLuaScript();
        } catch (Exception e) {
            logger.error("Redis连接初始化失败", e);
            throw new RuntimeException("Redis连接初始化失败", e);
        }
    }

    private static void preloadLuaScript() {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.ping(); // 测试连接
            luaScriptSha = jedis.scriptLoad(LUA_SCRIPT);
            logger.info("Lua脚本预加载成功，SHA: {}", luaScriptSha);
        } catch (Exception e) {
            logger.error("Lua脚本预加载失败", e);
            throw new RuntimeException("Lua脚本初始化失败", e);
        }
    }

    // 批量检查
    public static Map<String, Boolean> batchCheck(List<String> keywords) {
        if (keywords == null || keywords.isEmpty()) {
            logger.warn("传入空关键词列表");
            return Collections.emptyMap();
        }

        Map<String, Boolean> result = new HashMap<>(keywords.size());

        try (Jedis jedis = jedisPool.getResource()) {
            ensureScriptLoaded(jedis);

            Object response = executeScript(jedis, keywords);
            processResponse(keywords, response, result);
        } catch (Exception e) {
            handleError("批量检查", keywords.size(), e);
        }
        return result;
    }

    // 新增单个检查方法
    public static boolean checkSingle(String keyword) {
        if (keyword == null || keyword.isEmpty()) {
            logger.warn("传入空关键词");
            return false;
        }

        try (Jedis jedis = jedisPool.getResource()) {
            ensureScriptLoaded(jedis);

            Object response = executeScript(jedis, Collections.singletonList(keyword));
            return processSingleResponse(response);
        } catch (Exception e) {
            handleError("单条检查", 1, e);
            return false;
        }
    }

    private static void ensureScriptLoaded(Jedis jedis) {
        if (luaScriptSha == null) {
            synchronized (RedisLuaUtils.class) {
                if (luaScriptSha == null) {
                    luaScriptSha = jedis.scriptLoad(LUA_SCRIPT);
                    logger.info("运行时重新加载Lua脚本SHA: {}", luaScriptSha);
                }
            }
        }
    }

    private static Object executeScript(Jedis jedis, List<String> keywords) {
        try {
            return jedis.evalsha(luaScriptSha,
                    Collections.singletonList(SET_KEY), // KEYS
                    keywords // ARGV
            );
        } catch (JedisException e) {
            if (e.getMessage().contains("NOSCRIPT")) {
                logger.warn("Lua脚本未缓存，重新加载执行");
                return jedis.eval(LUA_SCRIPT,
                        Collections.singletonList(SET_KEY), // KEYS
                        keywords // ARGV
                );
            }
            throw e;
        }
    }

    private static void processResponse(List<String> keywords, Object response,
                                        Map<String, Boolean> result) {
        if (response instanceof List) {
            List<Long> rawResults = (List<Long>) response;
            for (int i = 0; i < keywords.size(); i++) {
                result.put(keywords.get(i), rawResults.get(i) == 1L);
            }
        }
    }

    private static boolean processSingleResponse(Object response) {
        if (response instanceof List && ((List<?>)response).size() == 1) {
            return ((Long)((List<?>)response).get(0)) == 1L;
        }
        throw new RuntimeException("无效的响应格式");
    }

    private static void handleError(String operationType, int keywordCount, Exception e) {
        String errorMsg = String.format("%s操作失败，关键词数量: %d", operationType, keywordCount);
        logger.error(errorMsg, e);
        throw new RuntimeException("敏感词检查服务暂不可用", e);
    }

    public boolean healthCheck() {
        try (Jedis jedis = jedisPool.getResource()) {
            return "PONG".equals(jedis.ping());
        } catch (Exception e) {
            logger.error("Redis健康检查失败", e);
            return false;
        }
    }

    public static void main(String[] args) {
        // 连接测试
        try (Jedis jedis = jedisPool.getResource()) {
            System.out.println("Redis服务器版本: " + jedis.info("server").split("\n")[0]);
            System.out.println("当前用户: " + jedis.aclWhoAmI());
        } catch (Exception e) {
            logger.error("初始连接测试失败", e);
            return;
        }
        // 测试批量检查
        List<String> testWords = Arrays.asList("真理部", "性爱体位", "胡金淘");
        Map<String, Boolean> batchResults = batchCheck(testWords);
        batchResults.forEach((word, exist) ->
                System.out.printf("[批量] 关键词 [%s] 存在: %s%n", word, exist ? "是" : "否"));

        // 测试单条检查
        testWords.forEach(word -> {
            boolean exists = checkSingle(word);
            System.out.printf("[单条] 关键词 [%s] 存在: %s%n", word, exists ? "是" : "否");
        });

        System.out.println("服务健康状态: " + (new RedisLuaUtils().healthCheck() ? "正常" : "异常"));
    }
}