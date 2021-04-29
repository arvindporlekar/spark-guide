package org.arvind.java.spark.core.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Tuples {

    public static void main(String[] args) {
        List<Double> doubleList = new ArrayList<>();
        doubleList.add(12.5);
        doubleList.add(123.4);
        doubleList.add(123.45);
        doubleList.add(444.567);

        SparkConf sparkConf = new SparkConf().setAppName("tuples-spark").setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<Double> originalRDD = javaSparkContext.parallelize(doubleList);

        JavaRDD<Tuple2<Double, Double>> modifiedRDD = originalRDD.map(aDouble -> new Tuple2<>(aDouble, Math.sqrt(aDouble)));

        modifiedRDD.collect().forEach(System.out::println);

    }
}
