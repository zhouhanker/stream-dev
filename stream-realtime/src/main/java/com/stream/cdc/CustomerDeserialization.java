package com.stream.cdc;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.HashMap;
import java.util.Map;

/**
 * @Package com.zh.flk.cdc.CustomerDeserialization
 * @Author zhou.han
 * @Date 2024/10/10 17:29
 * @description: cdc 自定义序列化
 */
public class CustomerDeserialization implements DebeziumDeserializationSchema<String> {
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        JSONObject parsedObject = new JSONObject();
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        // 获取不同表的id值
        Struct key = (Struct) sourceRecord.key();
        if (key.toString() != null && key.toString().length() > 0){
            String id = key.toString().split("=")[1].substring(0, key.toString().split("=")[1].length() - 1);
            parsedObject.put("id",id);
        }
        parsedObject.put("op", operation.toString());
        if (null != sourceRecord.topic()) {
            String[] splitTopic = sourceRecord.topic().split("\\.");
            if (splitTopic.length == 3) {
                parsedObject.put("database", splitTopic[1]);
                parsedObject.put("tableName", splitTopic[2]);
            }
        }
        Struct value = (Struct) sourceRecord.value();
        String tsMs = value.get("ts_ms").toString();
        // 变更前后的数据位于value这个Struct中，名称分别为before和after
        Struct before = value.getStruct("before");
        Struct after = value.getStruct("after");
        if (null != before) {
            Map<String, Object> beforeMap = new HashMap<>();
            Schema beforeSchema = before.schema();
            for (Field field : beforeSchema.fields()) {
                beforeMap.put(field.name(), before.get(field));
            }
            parsedObject.put("before", beforeMap.toString());
        }

        if (null != after) {
            Map<String, Object> afterMap = new HashMap<>();
            Schema afterSchema = after.schema();
            for (Field field : afterSchema.fields()) {
                afterMap.put(field.name(), after.get(field));
            }
            parsedObject.put("after", afterMap.toString());
        }
        parsedObject.put("id",parsedObject.get("id")+"_"+tsMs);
        parsedObject.put("ts",tsMs);
        collector.collect(parsedObject.toString());

    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
