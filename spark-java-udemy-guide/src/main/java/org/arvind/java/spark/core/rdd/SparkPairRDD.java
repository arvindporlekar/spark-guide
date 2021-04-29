package org.arvind.java.spark.core.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class SparkPairRDD {
    public static void main(String[] args) {
        List<String> inputData = new ArrayList<>();
        inputData.add("WARN: Tuesday 4 September 0405");
        inputData.add("ERROR: Tuesday 4 September 0408");
        inputData.add("FATAL: Wednesday 5 September 1632");
        inputData.add("ERROR: Friday 5 September 1854");
        inputData.add("WARN: Saturday 8 September 1942");

        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark-pair-rdd");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> javaRDD = javaSparkContext.parallelize(inputData);

        // printing all details by RDD
        JavaPairRDD<String, String> result = javaRDD.mapToPair(string -> {
            String[] column = string.split(":");
            String level = column[0];
            String date = column[1];

            return new Tuple2<>(level, date);
        });
        result.collect().forEach(System.out::println);

        // printing count of all logger level by reduce by key

        JavaPairRDD<String, Long> javaPairRDD = javaRDD.mapToPair(string -> {
            String[] resultArray = string.split(":");
            String logger = resultArray[0];
            return new Tuple2<>(logger, 1L);
        });

        JavaPairRDD<String, Long> resultReduceByKey = javaPairRDD.reduceByKey(Long::sum);
        resultReduceByKey.collect().forEach(System.out::println);
    }
}
