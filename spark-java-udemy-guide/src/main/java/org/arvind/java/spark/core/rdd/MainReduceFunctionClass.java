package org.arvind.java.spark.core.rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;

import java.util.ArrayList;
import java.util.List;

/**
 * Hello world!
 */
public class MainReduceFunctionClass {
    public static void main(String[] args) {
        List<Double> doubleList = new ArrayList<>();
        doubleList.add(12.5);
        doubleList.add(123.4);
        doubleList.add(123.45);
        doubleList.add(444.567);

        Logger.getLogger("org.apache").setLevel(Level.DEBUG);

        SparkConf sparkConf = new SparkConf().setSparkHome("Java-Spark-Udemy")
                .setAppName("java-udemy")
                .setMaster("local[*]");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<Double> doubleJavaRDD = javaSparkContext.parallelize(doubleList);

        Double finalValue  = doubleJavaRDD.reduce((aDouble, aDouble2) -> aDouble + aDouble2);

        javaSparkContext.close();
        System.out.println("Execution completed.  Total Sum =  " + finalValue);
    }
}
