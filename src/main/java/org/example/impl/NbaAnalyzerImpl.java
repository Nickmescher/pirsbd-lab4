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
     * Salaries содержит player_id, season_end, salary и team.
     * Stats содержит Year, Player, Age, Pos, PTS, TRB, AST; players — _id, name, birthDate.
     */
    @Override
    public Dataset<Row> costPerEfficiency(Dataset<Row> salaries, Dataset<Row> stats, Dataset<Row> players) {
        Dataset<Row> cleanStats = stats
                .withColumn("PTS", col("PTS"))
                .withColumn("TRB", col("TRB"))
                .withColumn("AST", col("AST").cast("double"))
                .withColumn("efficiency", col("PTS").plus(col("TRB")).plus(col("AST")))
                .filter(col("efficiency").gt(0))
                .select(
                        col("Year").cast("int").as("year"),
                        col("Player").as("player"),
                        col("Age").cast("int").as("age"),
                        col("Pos").as("pos"),
                        col("PTS"),
                        col("TRB"),
                        col("AST"),
                        col("efficiency")
                );

        Dataset<Row> cleanPlayers = players.select(
                col("_id").as("player_id"),
                col("name").as("player_name"),
                to_date(col("birthDate"), "MMMM d, yyyy").as("birth_date")
        ).filter(col("player_id").isNotNull().and(col("birth_date").isNotNull()));

        Dataset<Row> identities = cleanStats.select("year", "player", "age").distinct()
                .withColumn("reference_birth_date", make_date(col("year").minus(col("age")), lit(5), lit(1)));

        WindowSpec closestPlayerByAge = Window.partitionBy("year", "player", "age")
                .orderBy(
                        abs(datediff(col("birth_date"), col("reference_birth_date"))).asc(),
                        col("player_id").asc()
                );

        Dataset<Row> playerMapping = identities
                .join(cleanPlayers, identities.col("player").equalTo(cleanPlayers.col("player_name")), "inner")
                .filter(col("reference_birth_date").isNotNull())
                .withColumn("candidate_rank", row_number().over(closestPlayerByAge))
                .filter(col("candidate_rank").equalTo(1))
                .select("year", "player", "age", "player_id");

        Dataset<Row> mappedStats = cleanStats.join(playerMapping, new String[]{"year", "player", "age"}, "inner");

        Dataset<Row> cleanSalaries = salaries
                .withColumn("salary", col("salary").cast("double"))
                .filter(col("salary").isNotNull().and(col("player_id").isNotNull()))
                .select(
                        col("player_id"),
                        col("season_end").cast("int").as("year"),
                        col("salary"),
                        col("team")
                );

        return cleanSalaries
                .join(mappedStats, new String[]{"player_id", "year"}, "inner")
                .withColumn("cost_per_efficiency", col("salary").divide(col("efficiency")))
                .select(
                        col("year"),
                        col("player_id"),
                        col("player").as("player_name"),
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
