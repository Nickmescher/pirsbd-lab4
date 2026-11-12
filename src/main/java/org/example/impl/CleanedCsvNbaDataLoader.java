package org.example.impl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.example.NbaDataLoader;

public class CleanedCsvNbaDataLoader implements NbaDataLoader {

    @Override
    public Dataset<Row> loadPlayers(SparkSession spark, String path) {
        return readCsv(spark, path);
    }

    @Override
    public Dataset<Row> loadSalaries(SparkSession spark, String path) {
        return readCsv(spark, path);
    }

    @Override
    public Dataset<Row> loadSeasonStats(SparkSession spark, String path) {
        return readCsv(spark, path);
    }

    private Dataset<Row> readCsv(SparkSession spark, String path) {
        return spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .option("encoding", "UTF-8")
                .option("mode", "FAILFAST")
                .option("escape", "\"")
                .csv(path);
    }
}
