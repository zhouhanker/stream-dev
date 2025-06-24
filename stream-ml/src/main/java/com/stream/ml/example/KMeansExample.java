package com.stream.ml.example;

import org.apache.flink.ml.clustering.kmeans.KMeans;
import org.apache.flink.ml.clustering.kmeans.KMeansModel;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.linalg.Vectors;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

/**
 * @Package com.stream.ml.example.KMeansExample
 * @Author zhou.han
 * @Date 2025/6/9 20:06
 * @description:
 */
public class KMeansExample {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String featuresCol = "features";
        String predictionCol = "prediction";

        // Generate train data and predict data as DataStream.
        DataStream<DenseVector> inputStream = env.fromElements(
                Vectors.dense(0.0, 0.0),
                Vectors.dense(0.0, 0.3),
                Vectors.dense(0.3, 0.0),
                Vectors.dense(9.0, 0.0),
                Vectors.dense(9.0, 0.6),
                Vectors.dense(9.6, 0.0)
        );

        // Convert data from DataStream to Table, as Flink ML uses Table API.
        Table input = tEnv.fromDataStream(inputStream).as(featuresCol);

        // Creates a K-means object and initialize its parameters.
        KMeans kmeans = new KMeans()
                .setK(2)
                .setSeed(1L)
                .setFeaturesCol(featuresCol)
                .setPredictionCol(predictionCol);

        // Trains the K-means Model.
        KMeansModel model = kmeans.fit(input);

        // Use the K-means Model for predictions.
        Table output = model.transform(input)[0];

        // Extracts and displays prediction result.
        for (CloseableIterator<Row> it = output.execute().collect(); it.hasNext(); ) {
            Row row = it.next();
            DenseVector vector = (DenseVector) row.getField(featuresCol);
            int clusterId = (Integer) row.getField(predictionCol);
            System.out.println("Vector: " + vector + "\tCluster ID: " + clusterId);
        }
    }
}
