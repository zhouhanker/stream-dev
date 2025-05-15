import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.JdbcUtils;
import com.label.domain.DimBaseCategory;

import java.sql.Connection;
import java.util.List;

/**
 * @Package PACKAGE_NAME.JdbcTest
 * @Author zhou.han
 * @Date 2025/5/14 08:56
 * @description:
 */
public class JdbcTest {

    public static void main(String[] args) throws Exception {

        Connection connection = JdbcUtils.getMySQLConnection(
                ConfigUtils.getString("mysql.url"),
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"));


        String queryCategorySql = "select b3.id,             \n" +
                "            b3.name as b3name,              \n" +
                "            b2.name as b2name,              \n" +
                "            b1.name as b1name               \n" +
                "     from realtime_v1.base_category3 as b3  \n" +
                "     join realtime_v1.base_category2 as b2  \n" +
                "     on b3.category2_id = b2.id             \n" +
                "     join realtime_v1.base_category1 as b1  \n" +
                "     on b2.category1_id = b1.id";

        List<DimBaseCategory> dimBaseCategories = JdbcUtils.queryList2(connection, queryCategorySql, DimBaseCategory.class, false);
        for (DimBaseCategory dimBaseCategory : dimBaseCategories) {
            System.err.println(dimBaseCategory);
        }

    }
}
