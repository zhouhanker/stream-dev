import com.stream.common.utils.ConfigUtils;

import java.util.Date;

/**
 * @Package PACKAGE_NAME.Test
 * @Author zhou.han
 * @Date 2024/12/17 15:04
 * @description:
 */
public class Test {
    public static void main(String[] args) {
        System.err.println(ConfigUtils.getString("mysql.host"));
    }
}
