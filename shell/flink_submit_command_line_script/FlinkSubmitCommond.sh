# 应用模式提交任务 本地提交
./flink run-application \
-d \
-t yarn-application \
-ynm DbusCdc2DimHbase \
-yjm 900 \
-ytm 900 \
-yqu root.default \
-c com.retailersv1.DbusCdc2DimHbase /opt/soft/flink-1.17.1/local_jars/v2-project_1.jar

# 应用模式提交任务 HDFS提交
./flink run-application \
-d \
-t yarn-application \
-yjm 900 \
-ynm DbusLogDataProcess2Kafka \
-ytm 900 \
-yqu root.default \
-Dyarn.provided.lib.dirs="hdfs://cdh01:8020/flink-dist" \
-c com.retailersv1.DbusLogDataProcess2Kafka hdfs://cdh01:8020/flink-jars/DbusLogDataProcess2Kafka.jar

# Flink 保存SavePoint 命令
$ bin/flink savepoint :jobId [:targetDirectory] -yid :yarnAppId
# 使用SavePoint 停止Task
$ bin/flink stop --type [native/canonical] --savepointPath [:targetDirectory] :jobId
./flink stop  --savepointPath hdfs://cdh01:8020/flink-point/savepoint application_1733973984011_0166


