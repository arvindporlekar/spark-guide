package org.arvind.java.spark.core.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SparkFlatMap {
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

        javaRDD.flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .filter(s -> s.length() > 1)
                .collect()
                .forEach(System.out::println);
    }
}
