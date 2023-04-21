package com.example.exercice1rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

import java.util.Arrays;

@SpringBootApplication
public class Exercice1RddApplication {

    public static void main(String[] args) {
        // Configuration de Spark
        SparkConf conf = new SparkConf().setAppName("RDDLineageExample").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Création du premier RDD
        List<String> names = Arrays.asList("Ziad", "Ahmed", "Mounir", "Hamid", "Lina", "Samir", "Ranya");
        JavaRDD<String> rdd1 = sc.parallelize(names);

        // Transformation flatMap
        JavaRDD<String> rdd2 = rdd1.flatMap((FlatMapFunction<String, String>) s -> Arrays.asList(s.split(" ")).iterator());

        // Transformation filter
        JavaRDD<String> rdd3 = rdd2.filter(s -> s.startsWith("A"));
        JavaRDD<String> rdd4 = rdd2.filter(s -> s.startsWith("B"));
        JavaRDD<String> rdd5 = rdd2.filter(s -> s.startsWith("C"));

        // Transformation union
        JavaRDD<String> rdd6 = rdd3.union(rdd4);
        JavaRDD<String> rdd7 = rdd5.map(s -> s.toUpperCase());
        JavaPairRDD<String, Integer> rdd8 = rdd6.mapToPair((PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1))
                .reduceByKey((Function2<Integer, Integer, Integer>) Integer::sum)
                .mapToPair((PairFunction<Tuple2<String, Integer>, Integer, String>) Tuple2::swap)
                .sortByKey(false)
                .mapToPair((PairFunction<Tuple2<Integer, String>, String, Integer>) Tuple2::swap);

        JavaRDD<String> rdd9 = rdd7.union(rdd8.map(s -> s._2));
        JavaRDD<String> rdd10 = rdd8.union(rdd9).sortBy(s -> s, true, 1);

        // Affichage du résultat final
        System.out.println("RDD10:");
        for (String s : rdd10.collect()) {
            System.out.println(s);
        }

        // Arrêt de Spark
        sc.stop();
    }
}
