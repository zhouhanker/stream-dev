package com.stream.common.utils;

import java.util.List;
import java.util.regex.Pattern;

/**
 * @author weikaijun
 * @date 2022-06-07 13:28
 **/
public class StringsUtils {
    private StringsUtils() {
    }

    public static String toCsv(List<String> src) {
        // return src == null ? null : String.join(", ", src.toArray(new String[0]));
        return join(src, ", ");
    }

    public static String join(List<String> src, String delimiter) {
        return src == null ? null : String.join(delimiter, src.toArray(new String[0]));
    }

    public static String capitaliseFirstLetter(String string) {
        if (string == null || string.length() == 0) {
            return string;
        } else {
            return string.substring(0, 1).toUpperCase() + string.substring(1);
        }
    }

    public static String lowercaseFirstLetter(String string) {
        if (string == null || string.length() == 0) {
            return string;
        } else {
            return string.substring(0, 1).toLowerCase() + string.substring(1);
        }
    }

    public static String zeros(int n) {
        return repeat('0', n);
    }

    public static String repeat(char value, int n) {
        return new String(new char[n]).replace("\0", String.valueOf(value));
    }

    /**
     * Returns true if the string is empty, otherwise false.
     *
     * @param s String value
     * @return is given string is Empty or not
     */
    public static boolean isEmpty(String s) {
        return s == null || s.isEmpty();
    }

    /**
     * Returns true if the string is empty or contains only white space codepoints, otherwise false.
     *
     * @param s String value
     * @return is given string is Blank or not
     */
    public static boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }


    public static boolean matcherString(String arg) {
        String patternString = "^[\u4e00-\u9fa5a-zA-Z0-9\\[\\]\\-_+{}:：“<>`?!——+·~@#$%^&*(、，。；‘【】！@#￥%……&*（）)\"]+$";
        Pattern pattern = Pattern.compile(patternString);
        return pattern.matcher(arg).matches();
    }


}
