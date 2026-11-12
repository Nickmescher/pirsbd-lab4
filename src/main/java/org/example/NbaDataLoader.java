package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public interface NbaDataLoader {
    Dataset<Row> loadPlayers(SparkSession spark, String path);
    Dataset<Row> loadSalaries(SparkSession spark, String path);
    Dataset<Row> loadSeasonStats(SparkSession spark, String path);
}
