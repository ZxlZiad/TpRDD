package com.example.exercice2venteapp;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

@SpringBootApplication
public class Exercice2VenteappApplication {

    public static void main(String[] args) {

        // Configuration de Spark
        SparkConf conf = new SparkConf()
                .setAppName("VentesApp")
                .setMaster("local[*]");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        // Chargement du fichier ventes.txt
        JavaRDD<String> ventesRDD = sparkContext.textFile("ventes.txt");

        // Total des ventes par ville
        JavaPairRDD<String, Double> ventesParVilleRDD = ventesRDD.mapToPair(line -> {
            String[] tokens = line.split(" ");
            String ville = tokens[1];
            double prix = Double.parseDouble(tokens[3]);
            return new Tuple2<>(ville, prix);
        });
        JavaPairRDD<String, Double> totalVentesParVilleRDD = ventesParVilleRDD.reduceByKey((x, y) -> x + y);
        totalVentesParVilleRDD.saveAsTextFile("total_ventes_par_ville");

        // Prix total des ventes des produits par ville pour une année donnée
        JavaRDD<String> ventesAnneeRDD = ventesRDD.filter(line -> line.contains("annee"));
        JavaPairRDD<Tuple2<String, String>, Double> ventesParVilleProduitRDD = ventesAnneeRDD.mapToPair(line -> {
            String[] tokens = line.split(" ");
            String ville = tokens[1];
            String produit = tokens[2];
            double prix = Double.parseDouble(tokens[3]);
            return new Tuple2<>(new Tuple2<>(ville, produit), prix);
        });
        JavaPairRDD<Tuple2<String, String>, Double> totalVentesParVilleProduitRDD = ventesParVilleProduitRDD.reduceByKey((x, y) -> x + y);
        totalVentesParVilleProduitRDD.saveAsTextFile("total_ventes_par_ville_produit");

        // Arrêt de Spark
        sparkContext.stop();
    }

}
