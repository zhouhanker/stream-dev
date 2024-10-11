package com.stream.common.utils;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * 配置文件工具类
 * time: 2021/8/11 9:48 className: ConfigUtils.java
 *
 * @author han.zhou
 * @version 1.0.0
 */
public final class ConfigUtils {
    private static final Logger logger = LoggerFactory.getLogger(ConfigUtils.class);

    private static Properties properties;

    static {
        try {
            properties = new Properties();
            properties.load(ConfigUtils.class.getClassLoader().getResourceAsStream("common-config.properties"));
        } catch (IOException e) {
            logger.error("加载配置文件出错, exit 1", e);
            System.exit(1);
        }
    }

    public static String getString(String key) {
        // logger.info("加载配置[" + key + "]:" + value);
        return properties.getProperty(key).trim();
    }

    public static int getInt(String key) {
        String value = properties.getProperty(key).trim();
        return Integer.parseInt(value);
    }

    public static int getInt(String key, int defaultValue) {
        String value = properties.getProperty(key).trim();
        return Strings.isNullOrEmpty(value) ? defaultValue : Integer.parseInt(value);
    }

    public static long getLong(String key) {
        String value = properties.getProperty(key).trim();
        return Long.parseLong(value);
    }

    public static long getLong(String key, long defaultValue) {
        String value = properties.getProperty(key).trim();
        return Strings.isNullOrEmpty(value) ? defaultValue : Long.parseLong(value);
    }
}
