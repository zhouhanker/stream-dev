
import java.sql.*;

/**
 * @Package PACKAGE_NAME.ReadDorsiTest
 * @Author zhou.han
 * @Date 2025/2/12 18:01
 * @description:
 */

public class ReadDorisTest {
    // JDBC 连接参数
    private static final String JDBC_URL = "jdbc:mysql://10.39.48.33:9030/dev_t_zh";
    private static final String USER = "admin";
    private static final String PASSWORD = "zh1028,./";
    private static final String QUERY_SQL = "SELECT * FROM dev_t_zh.dws_trade_cart_add_uu_window LIMIT 10";

    public static void main(String[] args) {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");

            conn = DriverManager.getConnection(
                    JDBC_URL + "?serverTimezone=Asia/Shanghai&useSSL=false",
                    USER,
                    PASSWORD
            );

            stmt = conn.createStatement();
            rs = stmt.executeQuery(QUERY_SQL);
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                System.out.print(metaData.getColumnName(i) + "\t");
            }
            System.out.println();

            while (rs.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    System.out.print(rs.getString(i) + "\t");
                }
                System.out.println();
            }
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (rs != null) rs.close();
                if (stmt != null) stmt.close();
                if (conn != null) conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
