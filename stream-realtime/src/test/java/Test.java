import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.FlinkEnvUtils;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.Date;

/**
 * @Package PACKAGE_NAME.Test
 * @Author zhou.han
 * @Date 2024/12/17 15:04
 * @description:
 */
public class Test {
    // 给定一个int 数组，和一个int 值，返回该值在数组中的下标

    public static void main(String[] args) {

        int num = 4;
        int[] ints = new int[]{1,3,4,3,5};
        System.err.println(Arrays.binarySearch(ints, num));


    }


    public static int findIndexOfArr(int[] arr, int num){

        return  -1;
    }

}
