package com.example.weatheranalysis;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.apache.spark.sql.*;


@SpringBootTest
class WeatherAnalysisApplicationTests {

    public static void main(String[] args) {

        // Créer un objet SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("Weather Analysis")
                .master("local[*]")
                .getOrCreate();

        // Charger les données dans un DataFrame
        String filePath = "/path/to/weather_data.csv";
        Dataset<Row> weatherData = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv(filePath);

        // Effectuer des transformations sur le DataFrame
        Dataset<Row> filteredData = weatherData.filter("city = 'Paris' AND year = 2022");
        Dataset<Row> aggregatedData = filteredData.groupBy("day").agg(avg("temperature"), max("temperature"), min("temperature"));

        // Afficher les résultats de l'analyse
        System.out.println("Aggregated weather data:");
        aggregatedData.show();

        // Enregistrer les résultats dans un fichier CSV
        String outputPath = "/path/to/output/data.csv";
        aggregatedData.write().option("header", true).csv(outputPath);

        // Fermer la session Spark
        spark.close();
    }
}