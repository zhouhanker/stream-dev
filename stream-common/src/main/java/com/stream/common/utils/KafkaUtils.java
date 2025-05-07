package com.stream.common.utils;


import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.runtime.ValueSerializer;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
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
     *
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

    public static KafkaSource<String> buildKafkaSource(String bootServerList,String kafkaTopic,String group,OffsetsInitializer offset){
        return KafkaSource.<String>builder()
                .setBootstrapServers(bootServerList)
                .setTopics(kafkaTopic)
                .setGroupId(group)
                .setStartingOffsets(offset)
                .setValueOnlyDeserializer(new SimpleStringSchema())
                // 自动发现消费的partition变化
                .setProperty("flink.partition-discovery.interval-millis",String.valueOf(10 * 1000))
                .build();
    }

    public static KafkaSource<String> buildKafkaSecureSource(String bootServerList,String kafkaTopic,String group,OffsetsInitializer offset){
        return KafkaSource.<String>builder()
                .setBootstrapServers(bootServerList)
                .setTopics(kafkaTopic)
                .setGroupId(group)
                .setStartingOffsets(offset)
                .setValueOnlyDeserializer(new SafeStringDeserializationSchema())
                // 自动发现消费的partition变化
                .setProperty("flink.partition-discovery.interval-millis",String.valueOf(10 * 1000))
                .build();
    }



    public static KafkaSink<String> buildKafkaSink(String bootServerList, String kafkaTopic) {
        Properties producerProperties = new Properties();
        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootServerList);
        producerProperties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        producerProperties.setProperty(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));
        producerProperties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArraySerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArraySerializer.class.getName());

        System.err.println("Kafka Producer配置参数：");
        producerProperties.forEach((key, value) -> System.out.println(key + " = " + value));

        return KafkaSink.<String>builder()
                .setBootstrapServers(bootServerList)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(kafkaTopic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setKafkaProducerConfig(producerProperties)
                .build();
    }


    public static class SafeStringDeserializationSchema implements DeserializationSchema<String> {

        @Override
        public String deserialize(byte[] message) throws IOException {
            if (message == null) {
                return null;
            }
            return new String(message);
        }

        @Override
        public boolean isEndOfStream(String nextElement) {
            return false;
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return TypeInformation.of(String.class);
        }
    }


}
