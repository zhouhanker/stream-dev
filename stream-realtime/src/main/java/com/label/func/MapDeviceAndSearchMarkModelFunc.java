package com.label.func;

import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.JdbcUtils;
import com.label.domain.DimBaseCategory;
import com.label.domain.DimCategoryCompare;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Package com.retailersv1.func.MapDeviceMarkModel
 * @Author zhou.han
 * @Date 2025/5/13 21:34
 * @description: 设备打分模型
 */
public class MapDeviceAndSearchMarkModelFunc extends RichMapFunction<JSONObject,JSONObject> {

    private final double deviceRate;
    private final double searchRate;
    private final Map<String, DimBaseCategory> categoryMap;
    private List<DimCategoryCompare> dimCategoryCompares;
    private Connection connection;

    public MapDeviceAndSearchMarkModelFunc(List<DimBaseCategory> dimBaseCategories, double deviceRate,double searchRate) {
        this.deviceRate = deviceRate;
        this.searchRate = searchRate;
        this.categoryMap = new HashMap<>();
        // 将 DimBaseCategory 对象存储到 Map中  加快查询
        for (DimBaseCategory category : dimBaseCategories) {
            categoryMap.put(category.getB3name(), category);
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = JdbcUtils.getMySQLConnection(
                ConfigUtils.getString("mysql.url"),
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"));
        String sql = "select id, category_name, search_category from realtime_v1.category_compare_dic;";
        dimCategoryCompares = JdbcUtils.queryList2(connection, sql, DimCategoryCompare.class, true);
        super.open(parameters);
    }



    @Override
    public JSONObject map(JSONObject jsonObject) throws Exception {
        String os = jsonObject.getString("os");
        String[] labels = os.split(",");
        String judge_os = labels[0];
        jsonObject.put("judge_os", judge_os);

        if (judge_os.equals("iOS")) {
            jsonObject.put("device_18_24", round(0.7 * deviceRate));
            jsonObject.put("device_25_29", round(0.6 * deviceRate));
            jsonObject.put("device_30_34", round(0.5 * deviceRate));
            jsonObject.put("device_35_39", round(0.4 * deviceRate));
            jsonObject.put("device_40_49", round(0.3 * deviceRate));
            jsonObject.put("device_50",    round(0.2 * deviceRate));
        } else if (judge_os.equals("Android")) {
            jsonObject.put("device_18_24", round(0.8 * deviceRate));
            jsonObject.put("device_25_29", round(0.7 * deviceRate));
            jsonObject.put("device_30_34", round(0.6 * deviceRate));
            jsonObject.put("device_35_39", round(0.5 * deviceRate));
            jsonObject.put("device_40_49", round(0.4 * deviceRate));
            jsonObject.put("device_50",    round(0.3 * deviceRate));
        }

        String searchItem = jsonObject.getString("search_item");
        if (searchItem != null && !searchItem.isEmpty()) {
            DimBaseCategory category = categoryMap.get(searchItem);
            if (category != null) {
                jsonObject.put("b1_category", category.getB1name());
            }
        }
        // search
        String b1Category = jsonObject.getString("b1_category");
        if (b1Category != null && !b1Category.isEmpty()){
            for (DimCategoryCompare dimCategoryCompare : dimCategoryCompares) {
                if (b1Category.equals(dimCategoryCompare.getCategoryName())){
                    jsonObject.put("searchCategory",dimCategoryCompare.getSearchCategory());
                    break;
                }
            }
        }

        String searchCategory = jsonObject.getString("searchCategory");
        if (searchCategory == null) {
            searchCategory = "unknown";
        }
        switch (searchCategory) {
            case "时尚与潮流":
                jsonObject.put("search_18_24", round(0.9 * searchRate));
                jsonObject.put("search_25_29", round(0.7 * searchRate));
                jsonObject.put("search_30_34", round(0.5 * searchRate));
                jsonObject.put("search_35_39", round(0.3 * searchRate));
                jsonObject.put("search_40_49", round(0.2 * searchRate));
                jsonObject.put("search_50", round(0.1    * searchRate));
                break;
            case "性价比":
                jsonObject.put("search_18_24", round(0.2 * searchRate));
                jsonObject.put("search_25_29", round(0.4 * searchRate));
                jsonObject.put("search_30_34", round(0.6 * searchRate));
                jsonObject.put("search_35_39", round(0.7 * searchRate));
                jsonObject.put("search_40_49", round(0.8 * searchRate));
                jsonObject.put("search_50", round(0.8    * searchRate));
                break;
            case "健康与养生":
            case "家庭与育儿":
                jsonObject.put("search_18_24", round(0.1 * searchRate));
                jsonObject.put("search_25_29", round(0.2 * searchRate));
                jsonObject.put("search_30_34", round(0.4 * searchRate));
                jsonObject.put("search_35_39", round(0.6 * searchRate));
                jsonObject.put("search_40_49", round(0.8 * searchRate));
                jsonObject.put("search_50", round(0.7    * searchRate));
                break;
            case "科技与数码":
                jsonObject.put("search_18_24", round(0.8 * searchRate));
                jsonObject.put("search_25_29", round(0.6 * searchRate));
                jsonObject.put("search_30_34", round(0.4 * searchRate));
                jsonObject.put("search_35_39", round(0.3 * searchRate));
                jsonObject.put("search_40_49", round(0.2 * searchRate));
                jsonObject.put("search_50", round(0.1    * searchRate));
                break;
            case "学习与发展":
                jsonObject.put("search_18_24", round(0.4 * searchRate));
                jsonObject.put("search_25_29", round(0.5 * searchRate));
                jsonObject.put("search_30_34", round(0.6 * searchRate));
                jsonObject.put("search_35_39", round(0.7 * searchRate));
                jsonObject.put("search_40_49", round(0.8 * searchRate));
                jsonObject.put("search_50", round(0.7    * searchRate));
                break;
            default:
                jsonObject.put("search_18_24", 0);
                jsonObject.put("search_25_29", 0);
                jsonObject.put("search_30_34", 0);
                jsonObject.put("search_35_39", 0);
                jsonObject.put("search_40_49", 0);
                jsonObject.put("search_50", 0);
        }


        return jsonObject;

    }

    private static double round(double value) {
        return BigDecimal.valueOf(value)
                .setScale(3, RoundingMode.HALF_UP)
                .doubleValue();
    }


    @Override
    public void close() throws Exception {
        super.close();
        connection.close();
    }
}
