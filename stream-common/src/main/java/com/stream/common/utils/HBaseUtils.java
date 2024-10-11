package com.stream.common.utils;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.crypto.SecureUtil;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.stream.common.domain.HBaseInfo;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Executors;

import static org.apache.hadoop.hbase.CellUtil.cloneQualifier;
import static org.apache.hadoop.hbase.CellUtil.cloneValue;

/**
 * @author han.zhou
 * @time: 2021/10/14 11:39
 * @className: HBaseUtils
 * @description HBase 工具类
 */
public final class HBaseUtils {

    /**
     * 判断某个 rowkey 是否存在
     */
    public static boolean isExist(String tableName, String rowkey, Connection connection) throws Exception {
        Table table = connection.getTable(TableName.valueOf(tableName));
        return table.exists(new Get(Bytes.toBytes(rowkey)));
    }

    /**
     * 单条插入
     *
     * @param rowkey  主键
     * @param value   值
     * @param mutator
     * @throws IOException
     */
    public static void put(String rowkey, JSONObject value, BufferedMutator mutator) throws IOException {
        Put put = new Put(Bytes.toBytes(rowkey));
        for (Map.Entry<String, Object> entry : value.entrySet()) {
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(entry.getKey()), Bytes.toBytes(String.valueOf(entry.getValue())));
        }
        mutator.mutate(put);
    }

    /*
     * @param [rowkey, map, mutator]
     * @return void
     * @description 单条插入
     */
    public static void put(String rowkey, Map<String, Object> map, BufferedMutator mutator) throws IOException {
        Put put = new Put(Bytes.toBytes(rowkey));
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(entry.getKey()), Bytes.toBytes(String.valueOf(entry.getValue())));
        }
        mutator.mutate(put);
    }

    /**
     * @param tableName  表名
     * @param rowkey     rowkey
     * @param connection 连接
     * @param limitSize  不指定列族  最近时间倒序的limit交易数量 -1不做限制
     * @param families   列族数组。。。 指定列祖，返回该列族下最近时间倒序的limit交易数量   数量相乘
     */
    public static JSONObject getByRowkeyByLimit(String tableName, String rowkey, Connection connection, int limitSize, String... families) {

        long l = System.currentTimeMillis();
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName), Executors.newFixedThreadPool(10));
            Get get = new Get(rowkey.getBytes());

            if (families != null && families.length > 0) {
                for (String family : families) {
                    get.addFamily(family.getBytes());
                }
            }
            if (limitSize > 0) {
                get.setMaxResultsPerColumnFamily(limitSize);
            }
            Result result = table.get(get);
            if (result.isEmpty()) {
                return null;
            }
            JSONObject jsonObject = new JSONObject();
            result.listCells().forEach(cell -> {
                jsonObject.put(Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
            });
            return jsonObject;
        } catch (IOException e) {
            return new JSONObject();
        }
    }

    /**
     * 查询单条数据 某列的值
     *
     * @param tableName
     * @param rowkey
     * @param columnName
     * @return
     */
    public static String getColumnValue(String tableName, String rowkey, String columnName, Connection connection) throws Exception {
        String columnValue = null;
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowkey));
        Result result = table.get(get);
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            if (Bytes.toString(CellUtil.cloneQualifier(cell)).equals(columnName)) {
                columnValue = Bytes.toString(CellUtil.cloneValue(cell));
                break;
            }
        }
        return columnValue;
    }

    /**
     * 查询指定rowkey信息
     * 注：此方法只使用与所有列类型为string的场景
     *
     * @param tableName  表名
     * @param rowkey     主键
     * @param connection 连接
     * @return
     * @throws Exception
     */
    public static JSONObject getByRowkey(String tableName, String rowkey, Connection connection) throws Exception {
        Table table = connection.getTable(TableName.valueOf(tableName), Executors.newFixedThreadPool(10));
        Get get = new Get(Bytes.toBytes(rowkey));
        Result result = table.get(get);
        if (result.isEmpty()) {
            return null;
        }

        JSONObject jsonObject = new JSONObject();
        result.listCells().forEach(cell -> {
            jsonObject.put(Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
        });
        return jsonObject;
    }

    public static Map<String, JSONObject> getByRowKeys(String tableName, Collection<String> rowkeys, Connection connection) throws Exception {
        Table table = connection.getTable(TableName.valueOf(tableName), Executors.newFixedThreadPool(10));
        List<String> keys = new ArrayList<>(rowkeys);
        Map<String, JSONObject> res = new HashMap<>();

        for (List<String> keysTmp : Lists.partition(keys, 500)) {
            List<Row> batch = new ArrayList<>();
            for (String rowKey : keysTmp) {
                Get get = new Get(Bytes.toBytes(rowKey));
                batch.add(get);
            }
            Object[] results = new Object[batch.size()];
            table.batch(batch, results);
            for (Object item : results) {
                Result result = (Result) item;
                if (result.rawCells() == null || result.rawCells().length == 0) {
                    continue;
                }

                JSONObject jsonObject = new JSONObject();
                for (Cell cell : result.rawCells()) {
                    jsonObject.put(Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
                }
                res.put(Bytes.toString(result.getRow()), jsonObject);
            }
        }

        return res;
    }

    /**
     * 单条插入 - 某列
     *
     * @param rowkey      主键
     * @param columnName  列名
     * @param columnValue 列值
     * @param mutator
     * @throws Exception
     */
    public static void put(String rowkey, String columnName, String columnValue, BufferedMutator mutator) throws Exception {
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(columnName), Bytes.toBytes(columnValue));
        mutator.mutate(put);
    }

    /**
     * 单条插入 - 某列
     *
     * @param rowkey
     * @param columnFamily
     * @param columnName
     * @param columnValue
     * @param mutator
     * @throws Exception
     */
    public static void put(String rowkey, String columnFamily, String columnName, String columnValue, BufferedMutator mutator) throws Exception {
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(columnValue));
        mutator.mutate(put);
    }

    /**
     * 单条插入
     *
     * @param tableName  表名
     * @param rowkey     主键
     * @param value      值
     * @param connection 连接
     * @throws IOException
     */
    public static void put(String tableName, String rowkey, JSONObject value, Connection connection) throws IOException {
        Put put = new Put(Bytes.toBytes(rowkey));
        Table table = connection.getTable(TableName.valueOf(tableName), Executors.newFixedThreadPool(10));
        for (Map.Entry<String, Object> entry : value.entrySet()) {
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(entry.getKey()), Bytes.toBytes(String.valueOf(entry.getValue())));
        }
        table.put(put);
        table.close();
    }

    /**
     * 批量插入
     *
     * @param tableName
     * @param list
     * @param connection
     * @throws IOException
     */
    public static void batchPut4JSONObjectList(String tableName, List<JSONObject> list, Connection connection) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName), Executors.newFixedThreadPool(10));
        List<Put> puts = new ArrayList<>();
        for (JSONObject jsonObj : list) {
            String rowkey = jsonObj.getString("hash");
            if (rowkey != null) {
                Put put = new Put(Bytes.toBytes(rowkey));
                for (Map.Entry<String, Object> entry : jsonObj.entrySet()) {
                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(entry.getKey()), Bytes.toBytes(String.valueOf(entry.getValue())));
                }
                puts.add(put);
            }
        }
        table.put(puts);
        table.close();
    }


    /**
     * 批量插入
     *
     * @param tableName
     * @param columnFamily
     * @param list
     * @param connection
     * @throws IOException
     */
    public static void batchPut4TupleList(String tableName, String columnFamily,
                                          List<Tuple3<String, String, JSONObject>> list, Connection connection) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName), Executors.newFixedThreadPool(10));
        List<Put> puts = new ArrayList<>();
        for (Tuple3<String, String, JSONObject> tuple3 : list) {
            if (tuple3.f0 != null) {
                Put put = new Put(Bytes.toBytes(tuple3.f0));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(tuple3.f1), Bytes.toBytes(tuple3.f2.toJSONString()));
                puts.add(put);
            }
        }
        table.put(puts);
        table.close();
    }

    /**
     * 批量插入
     *
     * @param tableName
     * @param list
     * @param connection
     * @throws IOException
     */
    public static void batchPut4MapList(String tableName, List<Map<String, Object>> list, Connection connection) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName), Executors.newFixedThreadPool(10));
        List<Put> puts = new ArrayList<>();
        for (Map<String, Object> map : list) {
            String rowkey = (String) map.get("hash");
            Put put = new Put(Bytes.toBytes(rowkey));
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(entry.getKey()), Bytes.toBytes(String.valueOf(entry.getValue())));
            }
            puts.add(put);
        }
        table.put(puts);
        table.close();
    }

    /**
     * 批量插入
     *
     * @param tableName
     * @param rowkeys
     * @param columnName
     * @param columnValue
     */
    public static void batchPut4List(String tableName, List<String> rowkeys, String columnName, String columnValue, Connection connection) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName), Executors.newFixedThreadPool(10));
        List<Put> puts = new ArrayList<>();
        for (String rowkey : rowkeys) {
            Put put = new Put(Bytes.toBytes(rowkey));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(columnName), Bytes.toBytes(String.valueOf(columnValue)));
            puts.add(put);
        }
        table.put(puts);
        table.close();
    }

    public static HBaseInfo buildHbaseInfo(HBaseInfo.HBaseTable... tableList) {
        HBaseInfo hBaseInfo = new HBaseInfo();
        hBaseInfo.setAddress(ConfigUtils.getString("hbase.zookeeper.quorum"));
        hBaseInfo.setPort("2181");
        hBaseInfo.setTableList(Arrays.asList(tableList));
        return hBaseInfo;
    }

    public static String getRowKeyString(HBaseInfo.RowKey rowKey, JSONObject value) {
        String rowKeyString = null;
        List<String> rowKeyItemStringList = new ArrayList<>();
        for (HBaseInfo.RowKeyItem rowKeyItem : rowKey.getRowKeyItemList()) {
            String rowKeyItemString = value.getString(rowKeyItem.getFieldName());
            if (rowKeyItem.getRowKeyItemNeedMd5()) {
                rowKeyItemString = SecureUtil.md5(rowKeyItemString);
            }
            rowKeyItemStringList.add(rowKeyItemString);
        }
        if (!CollectionUtil.isEmpty(rowKeyItemStringList)) {
            rowKeyString = String.join(rowKey.getRowKeyItemFieldsSeparator(), rowKeyItemStringList);
            if (rowKey.getRowKeyNeedMd5()) {
                rowKeyString = SecureUtil.md5(rowKeyString);
            }
        }
        return rowKeyString;
    }

    public static JSONObject cellList2Obj(List<Cell> cells) {
        JSONObject jsonObject = new JSONObject();
        for (Cell cell : cells) {
            String qualifier = Bytes.toString(cloneQualifier(cell));
            String value = Bytes.toString(cloneValue(cell));
            jsonObject.put(qualifier, value);
        }
        return jsonObject;
    }

    public static Map<String, JSONObject> batchGet(Table table, Collection<String> keys) throws IOException {
        Map<String, JSONObject> res = new HashMap<>();
        List<Get> getList = new ArrayList<>();
        for (String key : keys) {
            getList.add(new Get(key.getBytes()));
        }

        Result[] results = table.get(getList);
        for (Result result : results) {
            if (result.rawCells() == null || result.rawCells().length == 0) {
                continue;
            }

            JSONObject jsonObject = new JSONObject();
            for (Cell cell : result.rawCells()) {
                jsonObject.put(Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
            }
            res.put(Bytes.toString(result.getRow()), jsonObject);
        }

        return res;
    }

}