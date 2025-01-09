package com.zh.deploy.utils;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * @Package com.zh.deploy.utils.deployFlinkUtils
 * @Author zhou.han
 * @Date 2025/1/5 23:19
 * @description: Flink deploy env and jars
 */
public class DeployFlinkUtils {

    private static final String python_conda_env  = "/Users/zhouhan/dev_env/conda/anaconda3/envs/stream-py/bin/python";
    private static final String python_file_path = "/Users/zhouhan/dev_env/work_project/java/stream-dev/stream-py/deploy/flink-deploy/PushFlinkJar2HdfsDir.py";


    @lombok.SneakyThrows
    public static void preparationEnvUploadJars(boolean isCommodShell,String fullClassName) {
        if (isCommodShell){
            ProcessBuilder processBuilder = new ProcessBuilder(python_conda_env, python_file_path, fullClassName);
            Process process = processBuilder.start();
            InputStream inputStream = process.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            String line;
            while ((line = reader.readLine())!= null) {
                System.out.println(line);
            }
            int exitValue = process.waitFor();
            System.out.println("Python脚本退出值: " + exitValue);
        }
    }

    public static String getClassName(String fullClassName) {
        String[] parts = fullClassName.split("\\.");
        if (parts.length > 0) {
            String lastName = parts[parts.length - 1];
            return lastName.trim();
        }
        return null;
    }

    public static void main(String[] args) {
        preparationEnvUploadJars(false,"com.retailersv1.DbusLogDataProcess2Kafka");
    }
}
