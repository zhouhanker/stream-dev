import com.zh.deploy.FlinkJobSubmitToYarnApplicationModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Package PACKAGE_NAME.TestLog
 * @Author zhou.han
 * @Date 2025/1/2 09:55
 * @description:
 */
public class TestLog {
    private static final Logger LOG = LoggerFactory.getLogger(TestLog.class.getName());
    public static void main(String[] args) {
        String applicationId = "application_1733973984011_0121";
        String webInterfaceURL = "http://cdh02:35290";
        LOG.info("\n\n" +
                "|-------------------------------<<applicationId>>-------------------------------|\n"+
                "|Flink Job Started ApplicationId: " + applicationId + "           \t\t|\n" +
                "|Flink Job Web Url: " + webInterfaceURL + "                        \t\t\t\t\t|\n" +
                "|_______________________________________________________________________________|");
    }
}
