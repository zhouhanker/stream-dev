import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.JdbcUtils;
import com.stream.domain.DimBaseCategory;
import com.stream.domain.DimCategoryCompare;

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

        String sql = "select id, category_name, search_category from realtime_v1.category_compare_dic;";

        List<DimCategoryCompare> dimCategoryCompares = JdbcUtils.queryList2(connection, sql, DimCategoryCompare.class, true);

        for (DimCategoryCompare dimCategoryCompare : dimCategoryCompares) {
            System.err.println(dimCategoryCompare);
        }


    }
}
