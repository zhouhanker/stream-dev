package com.stream.common.utils;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hbase.ClassFinder;

import java.io.*;
import java.net.URL;
import java.util.ArrayList;
import java.util.Properties;

/**
 * @Package com.stream.common.utils.FileUtils
 * @Author zhou.han
 * @Date 2024/11/20 10:48
 * @description: File Utils
 */
public class FileUtils {

    public static long getFileLastTime(String filePath){
        long time = 0;
        File file = new File(filePath);
        if (file.exists()){
            time = file.lastModified();
        }
        return time;
    }

    public static void sink2File(String path, String data){
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(path, false))){
            writer.write(data);
            writer.newLine();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static String getFileFirstLineData(String path){
        try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
            return reader.readLine();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static ArrayList<JSONObject> readFileData(String path){
        ArrayList<JSONObject> res = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(path))){
            String line ;
            while ((line = reader.readLine()) != null){
                JSONObject jsonObject = JSONObject.parseObject(line);
                res.add(jsonObject);
            }
        }catch (IOException ioException){
            ioException.printStackTrace();
        }
        return res;
    }

    public static String getResourceDicPath(){
        ClassLoader classLoader = FileUtils.class.getClassLoader();
        URL resource = classLoader.getResource("");
        if (resource != null){
            return resource.getPath();
        }
        return null;
    }

    public static void main(String[] args) {
        System.err.println(getResourceDicPath());
    }


}
