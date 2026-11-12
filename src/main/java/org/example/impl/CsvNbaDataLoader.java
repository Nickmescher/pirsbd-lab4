package org.example.impl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.example.NbaDataCleaner;
import org.example.NbaDataLoader;

public class CsvNbaDataLoader implements NbaDataLoader {

    private final NbaDataCleaner cleaner;

    public CsvNbaDataLoader() {
        this(new NbaDataCleanerImpl());
    }

    public CsvNbaDataLoader(NbaDataCleaner cleaner) {
        this.cleaner = cleaner;
    }

    @Override
    public Dataset<Row> loadPlayers(SparkSession spark, String path) {
        return cleaner.cleanPlayers(readCsv(spark, path));
    }

    @Override
    public Dataset<Row> loadSalaries(SparkSession spark, String path) {
        return cleaner.cleanSalaries(readCsv(spark, path));
    }

    @Override
    public Dataset<Row> loadSeasonStats(SparkSession spark, String path) {
        return cleaner.cleanStats(readCsv(spark, path));
    }

    private Dataset<Row> readCsv(SparkSession spark, String path) {
        return spark.read()
                .option("header", "true")
                .option("inferSchema", "false")
                .option("encoding", "UTF-8")
                .option("mode", "FAILFAST")
                .option("escape", "\"")
                .csv(path);
    }
}
