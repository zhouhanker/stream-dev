package com.stream.common.utils;

import cn.hutool.core.date.DateUtil;
import com.alibaba.fastjson.JSON;

import com.stream.common.domain.HdfsInfo;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * @author han.zhou
 **/
public class HdfsUtils {

    public static HdfsInfo buildHdfsInfo(String url, Boolean hdfsNeedPartition, String hdfsPartitionField) {

        HdfsInfo hdfsInfo = new HdfsInfo();
        hdfsInfo.setHdfsUrl(url);
        hdfsInfo.setHdfsNeedPartition(hdfsNeedPartition);
        hdfsInfo.setHdfsPartitionField(hdfsPartitionField);
        return hdfsInfo;
    }

    public static StreamingFileSink<String> getCommonSink(HdfsInfo hdfsInfo) {
        BucketAssigner<String, String> bucketAssigner;

        if (hdfsInfo.isHdfsNeedPartition()) {
            // 需要分区
            bucketAssigner = new BucketAssigner<String, String>() {
                @Override
                public String getBucketId(String input, Context context) {
                    String bucketId = "error_bucket";
                    try {
                        long timestamp = JSON.parseObject(input).getLongValue(hdfsInfo.getHdfsPartitionField());
                        if (timestamp < 10000000000L) {
                            timestamp = timestamp * 1000;
                        }
                        bucketId = "day=" + DateUtil.format(new Date(timestamp), "yyyyMMdd");
                        System.err.println(bucketId);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return bucketId;
                }

                @Override
                public SimpleVersionedSerializer<String> getSerializer() {
                    return SimpleVersionedStringSerializer.INSTANCE;
                }
            };
        } else { // 不需要分区
            bucketAssigner = new BasePathBucketAssigner<>();
        }

        return StreamingFileSink
                .forRowFormat(new Path(hdfsInfo.getHdfsUrl()), new SimpleStringEncoder<String>("UTF-8"))
                .withBucketAssigner(bucketAssigner)
                .withRollingPolicy(DefaultRollingPolicy.builder()
                        .withRolloverInterval(TimeUnit.MINUTES.toMillis(10))
                        .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                        .withMaxPartSize(1024 * 1024 * 128)
                        .build())
                .build();
    }
}
