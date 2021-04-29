package org.arvind.java.spark.core.rdd;

import com.google.common.collect.Iterables;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.collection.Iterable;

import java.util.ArrayList;
import java.util.List;

public class SparkPairRDDFinalCode {
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

        javaRDD.mapToPair(string-> new Tuple2<>(string.split(":")[0], 1L))
                .reduceByKey(Long::sum)
                .collect()
                .forEach(tuple -> System.out.println(tuple._1 + " has " + tuple._2  + " instances. "));

        // same with group by key
        javaRDD.mapToPair(string-> new Tuple2<>(string.split(":")[0], 1L))
                .groupByKey()
                .collect()
                .forEach(tuple -> System.out.println(tuple._1 + " has " + Iterables.size(tuple._2)  + " instances. "));  ;

    }
}
