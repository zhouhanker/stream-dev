package com.stream.common.utils;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

/**
 * time: 2021/8/11 10:20 className: KafkaUtils.java
 *
 * @author han.zhou
 * @version 1.0.0
 */
public final class KafkaUtils {

    /**
     * 构建基于字符串序列化的Kafka属性
     *
     * @param groupId 消费组ID
     * @return
     */
    public static Properties buildPropsStringDeserializer(String groupId) {
        final Properties props = new Properties();
        props.setProperty("bootstrap.servers", ConfigUtils.getString("bootstrap.servers"));
        System.err.println(ConfigUtils.getString("bootstrap.servers"));
        props.setProperty("group.id", groupId);
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("auto.offset.reset", "latest");
        //props.setProperty("auto.offset.reset", "earliest");
        return props;
    }

    public static Properties getKafkaConsumerProperties(String server, String groupId, String offset){
        Properties prop = new Properties();
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,server);
        prop.put(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,offset);
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        return prop;
    }

    /**
     * 构建生产者配置
     *
     * @return
     */
    public static Properties buildPropsByProducer() {
        final Properties props = new Properties();
        props.setProperty("bootstrap.servers", ConfigUtils.getString("bootstrap.servers"));
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("max.request.size", "10485760");
        return props;
    }
}
