package com.label.func;

import com.alibaba.fastjson.JSONObject;
import com.label.domain.DimSkuInfoMsg;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.JdbcUtils;
import com.label.domain.DimBaseCategory;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.util.List;

/**
 * @Package com.label.func.MapOrderAndDetailRateModelFunc
 * @Author zhou.han
 * @Date 2025/5/15 08:40
 * @description:
 */
public class MapOrderAndDetailRateModelFunc extends RichMapFunction<JSONObject,JSONObject> {

    private Connection connection;
    List<DimSkuInfoMsg> dimSkuInfoMsgs;

    private final List<DimBaseCategory> dimBaseCategories;
    private final double timeRate;
    private final double amountRate;
    private final double brandRate;
    private final double categoryRate;


    public MapOrderAndDetailRateModelFunc(List<DimBaseCategory> dimBaseCategories, double timeRate, double amountRate, double brandRate, double categoryRate) {
        this.dimBaseCategories = dimBaseCategories;
        this.timeRate = timeRate;
        this.amountRate = amountRate;
        this.brandRate = brandRate;
        this.categoryRate = categoryRate;
    }

    @Override
    public void open(Configuration parameters) throws Exception {

        connection = JdbcUtils.getMySQLConnection(
                ConfigUtils.getString("mysql.url"),
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"));

        String querySkuSql = "select sku_info.id as skuid,      \n" +
                        "       spu_info.id as spuid,           \n" +
                        "       spu_info.category3_id as c3id,  \n" +
                        "       base_trademark.tm_name as tname \n" +
                        "from realtime_v1.sku_info              \n" +
                        "join realtime_v1.spu_info              \n" +
                        "on sku_info.spu_id = spu_info.id       \n" +
                        "join realtime_v1.base_trademark        \n" +
                        "on realtime_v1.spu_info.tm_id = realtime_v1.base_trademark.id";

        dimSkuInfoMsgs = JdbcUtils.queryList2(connection, querySkuSql, DimSkuInfoMsg.class);

    }



    @Override
    public JSONObject map(JSONObject jsonObject) throws Exception {
        String skuId = jsonObject.getString("sku_id");
        if (skuId != null && !skuId.isEmpty()){
            for (DimSkuInfoMsg dimSkuInfoMsg : dimSkuInfoMsgs) {
                if (dimSkuInfoMsg.getSkuid().equals(skuId)){
                    jsonObject.put("c3id",dimSkuInfoMsg.getC3id());
                    jsonObject.put("tname",dimSkuInfoMsg.getTname());
                    break;
                }
            }
        }

        String c3id = jsonObject.getString("c3id");
        if (c3id != null && !c3id.isEmpty()){
            for (DimBaseCategory dimBaseCategory : dimBaseCategories) {
                if (c3id.equals(dimBaseCategory.getId())){
                    jsonObject.put("b1_name",dimBaseCategory.getB1name());
                    break;
                }
            }
        }

        // 时间打分
        String payTimeSlot = jsonObject.getString("pay_time_slot");
        if (payTimeSlot != null && !payTimeSlot.isEmpty()){
            switch (payTimeSlot) {
                case "凌晨":
                    jsonObject.put("pay_time_18-24", round(0.2 * timeRate));
                    jsonObject.put("pay_time_25-29", round(0.1 * timeRate));
                    jsonObject.put("pay_time_30-34", round(0.1 * timeRate));
                    jsonObject.put("pay_time_35-39", round(0.1 * timeRate));
                    jsonObject.put("pay_time_40-49", round(0.1 * timeRate));
                    jsonObject.put("pay_time_50", round(0.1 * timeRate));
                    break;
                case "早晨":
                    jsonObject.put("pay_time_18-24", round(0.1 * timeRate));
                    jsonObject.put("pay_time_25-29", round(0.1 * timeRate));
                    jsonObject.put("pay_time_30-34", round(0.1 * timeRate));
                    jsonObject.put("pay_time_35-39", round(0.1 * timeRate));
                    jsonObject.put("pay_time_40-49", round(0.2 * timeRate));
                    jsonObject.put("pay_time_50", round(0.3 * timeRate));
                    break;
                case "上午":
                    jsonObject.put("pay_time_18-24", round(0.2 * timeRate));
                    jsonObject.put("pay_time_25-29", round(0.2 * timeRate));
                    jsonObject.put("pay_time_30-34", round(0.2 * timeRate));
                    jsonObject.put("pay_time_35-39", round(0.2 * timeRate));
                    jsonObject.put("pay_time_40-49", round(0.3 * timeRate));
                    jsonObject.put("pay_time_50", round(0.4 * timeRate));
                    break;
                case "中午":
                    jsonObject.put("pay_time_18-24", round(0.4 * timeRate));
                    jsonObject.put("pay_time_25-29", round(0.4 * timeRate));
                    jsonObject.put("pay_time_30-34", round(0.4 * timeRate));
                    jsonObject.put("pay_time_35-39", round(0.4 * timeRate));
                    jsonObject.put("pay_time_40-49", round(0.4 * timeRate));
                    jsonObject.put("pay_time_50", round(0.3 * timeRate));
                    break;
                case "下午":
                    jsonObject.put("pay_time_18-24", round(0.4 * timeRate));
                    jsonObject.put("pay_time_25-29", round(0.5 * timeRate));
                    jsonObject.put("pay_time_30-34", round(0.5 * timeRate));
                    jsonObject.put("pay_time_35-39", round(0.5 * timeRate));
                    jsonObject.put("pay_time_40-49", round(0.5 * timeRate));
                    jsonObject.put("pay_time_50", round(0.4 * timeRate));
                    break;
                case "晚上":
                    jsonObject.put("pay_time_18-24", round(0.8 * timeRate));
                    jsonObject.put("pay_time_25-29", round(0.7 * timeRate));
                    jsonObject.put("pay_time_30-34", round(0.6 * timeRate));
                    jsonObject.put("pay_time_35-39", round(0.5 * timeRate));
                    jsonObject.put("pay_time_40-49", round(0.4 * timeRate));
                    jsonObject.put("pay_time_50", round(0.3 * timeRate));
                    break;
                case "夜间":
                    jsonObject.put("pay_time_18-24", round(0.9 * timeRate));
                    jsonObject.put("pay_time_25-29", round(0.7 * timeRate));
                    jsonObject.put("pay_time_30-34", round(0.5 * timeRate));
                    jsonObject.put("pay_time_35-39", round(0.3 * timeRate));
                    jsonObject.put("pay_time_40-49", round(0.2 * timeRate));
                    jsonObject.put("pay_time_50", round(0.1 * timeRate));
                    break;
            }
        }

        // 价格打分
        double totalAmount = jsonObject.getDoubleValue("total_amount");
        if (totalAmount < 1000){
            jsonObject.put("amount_18-24", round(0.8 * amountRate));
            jsonObject.put("amount_25-29", round(0.6 * amountRate));
            jsonObject.put("amount_30-34", round(0.4 * amountRate));
            jsonObject.put("amount_35-39", round(0.3 * amountRate));
            jsonObject.put("amount_40-49", round(0.2 * amountRate));
            jsonObject.put("amount_50",    round(0.1 * amountRate));
            jsonObject.put("spending_pow", "低消费");
        }else if (totalAmount > 1000 && totalAmount < 4000){
            jsonObject.put("amount_18-24", round(0.2 * amountRate));
            jsonObject.put("amount_25-29", round(0.4 * amountRate));
            jsonObject.put("amount_30-34", round(0.6 * amountRate));
            jsonObject.put("amount_35-39", round(0.7 * amountRate));
            jsonObject.put("amount_40-49", round(0.8 * amountRate));
            jsonObject.put("amount_50",    round(0.7 * amountRate));
            jsonObject.put("spending_pow", "中消费");
        }else {
            jsonObject.put("amount_18-24", round(0.1 * amountRate));
            jsonObject.put("amount_25-29", round(0.2 * amountRate));
            jsonObject.put("amount_30-34", round(0.3 * amountRate));
            jsonObject.put("amount_35-39", round(0.4 * amountRate));
            jsonObject.put("amount_40-49", round(0.5 * amountRate));
            jsonObject.put("amount_50",    round(0.6 * amountRate));
            jsonObject.put("spending_pow", "高消费");
        }


        // 品牌
        String tname = jsonObject.getString("tname");
        if (tname != null && !tname.isEmpty()){
            switch (tname) {
                case "TCL":
                    jsonObject.put("tname_18-24", round(0.2 * brandRate));
                    jsonObject.put("tname_25-29", round(0.3 * brandRate));
                    jsonObject.put("tname_30-34", round(0.4 * brandRate));
                    jsonObject.put("tname_35-39", round(0.5 * brandRate));
                    jsonObject.put("tname_40-49", round(0.6 * brandRate));
                    jsonObject.put("tname_50", round(0.7 * brandRate));
                    break;
                case "苹果":
                case "联想":
                case "小米":
                    jsonObject.put("tname_18-24", round(0.9 * brandRate));
                    jsonObject.put("tname_25-29", round(0.8 * brandRate));
                    jsonObject.put("tname_30-34", round(0.7 * brandRate));
                    jsonObject.put("tname_35-39", round(0.7 * brandRate));
                    jsonObject.put("tname_40-49", round(0.7 * brandRate));
                    jsonObject.put("tname_50", round(0.5 * brandRate));
                    break;
                case "欧莱雅":
                    jsonObject.put("tname_18-24", round(0.5 * brandRate));
                    jsonObject.put("tname_25-29", round(0.6 * brandRate));
                    jsonObject.put("tname_30-34", round(0.8 * brandRate));
                    jsonObject.put("tname_35-39", round(0.8 * brandRate));
                    jsonObject.put("tname_40-49", round(0.9 * brandRate));
                    jsonObject.put("tname_50", round(0.2 * brandRate));
                    break;
                case "香奈儿":
                    jsonObject.put("tname_18-24", round(0.3 * brandRate));
                    jsonObject.put("tname_25-29", round(0.4 * brandRate));
                    jsonObject.put("tname_30-34", round(0.6 * brandRate));
                    jsonObject.put("tname_35-39", round(0.8 * brandRate));
                    jsonObject.put("tname_40-49", round(0.9 * brandRate));
                    jsonObject.put("tname_50", round(0.2 * brandRate));
                    break;
                default:
                    jsonObject.put("tname_18-24", round(0.1 * brandRate));
                    jsonObject.put("tname_25-29", round(0.2 * brandRate));
                    jsonObject.put("tname_30-34", round(0.3 * brandRate));
                    jsonObject.put("tname_35-39", round(0.4 * brandRate));
                    jsonObject.put("tname_40-49", round(0.5 * brandRate));
                    jsonObject.put("tname_50", round(0.6 * brandRate));
                    break;
            }
        }

        // 类目

        String b1Name = jsonObject.getString("b1_name");
        if (b1Name != null && !b1Name.isEmpty()){
            switch (b1Name){
                case "数码":
                case "手机":
                case "电脑办公":
                case "个护化妆":
                case "服饰内衣":
                    jsonObject.put("b1name_18-24", round(0.9 * categoryRate));
                    jsonObject.put("b1name_25-29", round(0.8 * categoryRate));
                    jsonObject.put("b1name_30-34", round(0.6 * categoryRate));
                    jsonObject.put("b1name_35-39", round(0.4 * categoryRate));
                    jsonObject.put("b1name_40-49", round(0.2 * categoryRate));
                    jsonObject.put("b1name_50",    round(0.1 * categoryRate));
                    break;
                case "家居家装":
                case "图书、音像、电子书刊":
                case "厨具":
                case "鞋靴":
                case "母婴":
                case "汽车用品":
                case "珠宝":
                case "家用电器":
                    jsonObject.put("b1name_18-24", round(0.2 * categoryRate));
                    jsonObject.put("b1name_25-29", round(0.4 * categoryRate));
                    jsonObject.put("b1name_30-34", round(0.6 * categoryRate));
                    jsonObject.put("b1name_35-39", round(0.8 * categoryRate));
                    jsonObject.put("b1name_40-49", round(0.9 * categoryRate));
                    jsonObject.put("b1name_50",    round(0.7 * categoryRate));
                    break;
                default:
                    jsonObject.put("b1name_18-24", round(0.1 * categoryRate));
                    jsonObject.put("b1name_25-29", round(0.2 * categoryRate));
                    jsonObject.put("b1name_30-34", round(0.4 * categoryRate));
                    jsonObject.put("b1name_35-39", round(0.5 * categoryRate));
                    jsonObject.put("b1name_40-49", round(0.8 * categoryRate));
                    jsonObject.put("b1name_50",    round(0.9 * categoryRate));
            }
        }


        return jsonObject;
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }

    private static double round(double value) {
        return BigDecimal.valueOf(value)
                .setScale(3, RoundingMode.HALF_UP)
                .doubleValue();
    }

}
