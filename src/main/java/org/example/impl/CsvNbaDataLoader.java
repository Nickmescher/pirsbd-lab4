package org.example.impl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.example.NbaDataLoader;

public class CsvNbaDataLoader implements NbaDataLoader {

    @Override
    public Dataset<Row> loadPlayers(SparkSession spark, String path) {
        return spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(path);
    }

    @Override
    public Dataset<Row> loadSalaries(SparkSession spark, String path) {
        return spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(path);
    }

    @Override
    public Dataset<Row> loadSeasonStats(SparkSession spark, String path) {
        return spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(path);
    }
}
