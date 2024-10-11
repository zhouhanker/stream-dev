package com.stream.common.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 日期工具类
 * time: 2021/9/9 13:37 className: DateTimeUtils.java
 *
 * @author han.zhou
 * @version 1.0.0
 */
public final class DateTimeUtils {
    public final static String YYYY_MM_DD_HH_MM_SS = "yyyy-MM-dd HH:mm:ss";

    public static String format(Date date) {
        return format(date, YYYY_MM_DD_HH_MM_SS);
    }

    public static String format(Date date, String format) {
        SimpleDateFormat formatter = new SimpleDateFormat(format);
        return formatter.format(date);
    }
}
