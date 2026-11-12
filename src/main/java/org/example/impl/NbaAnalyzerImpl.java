package org.example.impl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.example.NbaAnalyzer;

import static org.apache.spark.sql.functions.*;

public class NbaAnalyzerImpl implements NbaAnalyzer {

    /**
     * Считает efficiency = PTS + TRB + AST и cost_per_efficiency = salary / efficiency.
     * Salaries должен содержать колонки: player_name, year (int), salary (double).
     * Stats — оригинальный Seasons_Stats.csv с колонками Year, Player, PTS, TRB, AST.
     */
    @Override
    public Dataset<Row> costPerEfficiency(Dataset<Row> salaries, Dataset<Row> stats) {
        Dataset<Row> cleanStats = stats
                .withColumn("PTS", coalesce(col("PTS").cast("double"), lit(0.0)))
                .withColumn("TRB", coalesce(col("TRB").cast("double"), lit(0.0)))
                .withColumn("AST", coalesce(col("AST").cast("double"), lit(0.0)))
                .withColumn("efficiency", col("PTS").plus(col("TRB")).plus(col("AST")))
                .filter(col("efficiency").gt(0))
                .select(
                        col("Year").cast("int").as("year"),
                        col("Player").as("player"),
                        col("Pos").as("pos"),
                        col("PTS"),
                        col("TRB"),
                        col("AST"),
                        col("efficiency")
                );

        Dataset<Row> cleanSalaries = salaries
                .withColumn("salary", col("salary").cast("double"))
                .filter(col("salary").isNotNull().and(col("player_name").isNotNull()))
                .select(
                        col("player_name"),
                        col("year"),
                        col("salary"),
                        col("team")
                );

        return cleanSalaries
                .join(cleanStats,
                        cleanSalaries.col("year").equalTo(cleanStats.col("year"))
                                .and(cleanSalaries.col("player_name").equalTo(cleanStats.col("player"))),
                        "inner")
                .withColumn("cost_per_efficiency", col("salary").divide(col("efficiency")))
                .select(
                        cleanStats.col("year"),
                        col("player_name"),
                        col("pos"),
                        col("team"),
                        col("salary"),
                        col("PTS"),
                        col("TRB"),
                        col("AST"),
                        col("efficiency"),
                        col("cost_per_efficiency")
                );
    }

    /**
     * Топ-N игроков с наименьшим cost_per_efficiency за каждый год.
     * Использует window-функцию rank() по возрастанию cost_per_efficiency.
     */
    @Override
    public Dataset<Row> topCheapestByYear(Dataset<Row> costPerEfficiency, int topN) {
        WindowSpec w = Window.partitionBy("year").orderBy(col("cost_per_efficiency").asc());

        return costPerEfficiency
                .withColumn("rank", rank().over(w))
                .filter(col("rank").leq(topN))
                .orderBy(col("year"), col("rank"));
    }
}
