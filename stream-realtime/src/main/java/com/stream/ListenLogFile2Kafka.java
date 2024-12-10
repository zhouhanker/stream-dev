package com.stream;

import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.FileUtils;
import com.stream.common.utils.KafkaUtils;
import java.util.Objects;

/**
 * @Package com.stream.ListenLogFile2Kafka
 * @Author zhou.han
 * @Date 2024/11/20 10:42
 * @description: 监听日志数据生成 并发送给kafka
 */
public class ListenLogFile2Kafka {
    private static final String REALTIME_LOG_FILE_PATH = ConfigUtils.getString("REALTIME.LOG.FILE.PATH");
    private static final String REALTIME_MSG_POSITION_FILE_PATH = ConfigUtils.getString("REALTIME.MSG.POSITION.FILE.PATH");
    private static final String REALTIME_KAFKA_LOG_TOPIC = ConfigUtils.getString("REALTIME.KAFKA.LOG.TOPIC");


    public static void main(String[] args) {

        if (Long.parseLong(Objects.requireNonNull(FileUtils.getFileFirstLineData(REALTIME_MSG_POSITION_FILE_PATH)))
                <
                FileUtils.getFileLastTime(REALTIME_LOG_FILE_PATH)){
            KafkaUtils.sinkJson2KafkaMessage(REALTIME_KAFKA_LOG_TOPIC,FileUtils.readFileData(REALTIME_LOG_FILE_PATH));
            FileUtils.sink2File(REALTIME_MSG_POSITION_FILE_PATH,String.valueOf(FileUtils.getFileLastTime(REALTIME_LOG_FILE_PATH)));
        }else {
            System.out.println("Message Log Is Last Data !");
        }

    }



}
