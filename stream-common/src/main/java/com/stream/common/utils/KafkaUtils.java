package com.stream.common.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.ArrayList;
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
        props.setProperty("auto.offset.reset", "earliest");
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
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ConfigUtils.getString("kafka.bootstrap.servers"));
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("max.request.size", "10485760");
        return props;
    }

    public static void sinkJson2KafkaMessage(String topicName, ArrayList<JSONObject> jsonObjectArrayList){
        Properties properties = buildPropsByProducer();
        try(KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            for (JSONObject jsonObject : jsonObjectArrayList) {
                producer.send(new ProducerRecord<>(topicName,jsonObject.toString()));
            }
            System.out.println("数据已成功发送到Kafka主题: " + topicName);
        }catch (Exception e){
            e.printStackTrace();
            System.out.println("发送数据到Kafka主题时出现错误");
        }
    }
}
