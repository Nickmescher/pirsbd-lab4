package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.example.impl.CsvNbaDataLoader;
import org.example.impl.NbaAnalyzerImpl;

import static org.apache.spark.sql.functions.*;

public class NbaApp {

    private static final int DEFAULT_TOP_N = 5;

    public static void main(String[] args) {
        if (args.length < 4) {
            System.err.println(
                    "Usage: NbaApp <playersPath> <salariesPath> <statsPath> <outputDir> [topN]");
            System.exit(1);
        }

        String playersPath = args[0];
        String salariesPath = args[1];
        String statsPath = args[2];
        String outputDir = args[3];
        int topN = args.length >= 5 ? Integer.parseInt(args[4]) : DEFAULT_TOP_N;

        SparkSession spark = SparkSession.builder()
                .appName("NbaApp")
                .config("spark.sql.shuffle.partitions", "16")
                .getOrCreate();

        NbaDataLoader loader = new CsvNbaDataLoader();
        NbaAnalyzer analyzer = new NbaAnalyzerImpl();

        Dataset<Row> players = loader.loadPlayers(spark, playersPath);
        Dataset<Row> salaries = loader.loadSalaries(spark, salariesPath);
        Dataset<Row> stats = loader.loadSeasonStats(spark, statsPath);

        // Обогащаем зарплаты именем игрока через таблицу players (_id -> player_id)
        Dataset<Row> salariesWithName = salaries
                .join(players, salaries.col("player_id").equalTo(players.col("_id")), "left")
                .select(
                        players.col("name").as("player_name"),
                        salaries.col("salary"),
                        salaries.col("season_start").cast("int").as("year"),
                        salaries.col("team")
                );

        // Считаем efficiency и cost_per_efficiency
        Dataset<Row> cpe = analyzer.costPerEfficiency(salariesWithName, stats);

        // Сохраняем в Parquet с партиционированием по году
        String parquetPath = outputDir + "/cost_per_efficiency";
        cpe.write()
                .mode("overwrite")
                .partitionBy("year")
                .parquet(parquetPath);

        // Читаем из паркета и выводим топ-N по каждому году
        Dataset<Row> fromParquet = spark.read().parquet(parquetPath);
        Dataset<Row> top = analyzer.topCheapestByYear(fromParquet, topN);

        top.coalesce(1)
                .write()
                .option("header", "true")
                .mode("overwrite")
                .csv(outputDir + "/top_cheapest_by_year");

        top.show(topN * 10, false);

        spark.stop();
    }
}
