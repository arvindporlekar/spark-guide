package org.arvind.java.spark.core.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class SparkReadingFile {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark-local-file");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> javaRDD = javaSparkContext.textFile("src/main/resources/subtitles/input.txt");

        javaRDD.flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .filter(s -> s.length() > 1)
                .collect()
                .forEach(System.out::println);


    }
}
