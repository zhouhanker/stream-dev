package com.v3;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.source.SourceRecord;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.util.HashMap;
import java.util.Map;


/**
 * @Package com.v3.CustomerDeserializationSchemaSqlserver
 * @Author zhou.han
 * @Date 2025/7/23 15:28
 * @description:
 */
public class CustomerDeserializationSchemaSqlserver implements DebeziumDeserializationSchema<String> {
    private static final long serialVersionUID = -1L;
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        Map<String, Object> resultMap = new HashMap<>();
        String topic = sourceRecord.topic();
        String[] split = topic.split("[.]");
        String database = split[1];
        String table = split[2];
        resultMap.put("db", database);
        resultMap.put("tableName", table);
        //获取操作类型
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        //获取数据本身
        Struct struct = (Struct) sourceRecord.value();
        Struct after = struct.getStruct("after");
        Struct before = struct.getStruct("before");
        String op = operation.name();
        resultMap.put("op", op);

        //新增,更新或者初始化
        if (op.equals(Envelope.Operation.CREATE.name()) || op.equals(Envelope.Operation.READ.name()) || op.equals(Envelope.Operation.UPDATE.name())) {
            JSONObject afterJson = new JSONObject();
            if (after != null) {
                Schema schema = after.schema();
                for (Field field : schema.fields()) {
                    afterJson.put(field.name(), after.get(field.name()));
                }
                resultMap.put("after", afterJson);
            }
        }

        if (op.equals(Envelope.Operation.DELETE.name())) {
            JSONObject beforeJson = new JSONObject();
            if (before != null) {
                Schema schema = before.schema();
                for (Field field : schema.fields()) {
                    beforeJson.put(field.name(), before.get(field.name()));
                }
                resultMap.put("before", beforeJson);
            }
        }

//        collector.collect(JSON.toJSONString(resultMap, JSONWriter.Feature.FieldBased, JSONWriter.Feature.LargeObject));
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
