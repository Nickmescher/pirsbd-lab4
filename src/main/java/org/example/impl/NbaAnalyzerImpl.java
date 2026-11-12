package org.example.impl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.example.NbaAnalyzer;

import static org.apache.spark.sql.functions.*;

public class NbaAnalyzerImpl implements NbaAnalyzer {

    /**
     * Считает годовую efficiency = PTS + TRB + AST из строк TOT и cost_per_efficiency = salary / efficiency.
     * salary — сумма зарплат за сезон, делённая на число различных Tm в stats, исключая TOT.
     * Salaries содержит player_id, season_end и salary.
     * Stats содержит Year, Player, Age, Tm, PTS, TRB, AST; players — _id, name, birthDate.
     */
    @Override
    public Dataset<Row> costPerEfficiency(Dataset<Row> salaries, Dataset<Row> stats, Dataset<Row> players) {
        Dataset<Row> cleanStats = stats
                .withColumn("PTS", col("PTS").cast("double"))
                .withColumn("TRB", col("TRB").cast("double"))
                .withColumn("AST", col("AST").cast("double"))
                .select(
                        col("Year").cast("int").as("year"),
                        col("Player").as("player"),
                        col("Age").cast("int").as("age"),
                        col("Tm").as("stats_team"),
                        col("PTS"),
                        col("TRB"),
                        col("AST")
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

        // Count actual teams before selecting TOT rows for the season's performance totals.
        Dataset<Row> statsTeamCounts = mappedStats
                .groupBy("player_id", "year")
                .agg(countDistinct(when(col("stats_team").notEqual("TOT"), col("stats_team"))).as("team_count"));

        Dataset<Row> yearlyStats = mappedStats
                .filter(col("stats_team").equalTo("TOT"))
                .select(
                        col("player_id"),
                        col("year"),
                        col("player").as("player_name"),
                        col("PTS"),
                        col("TRB"),
                        col("AST")
                )
                .withColumn("efficiency", col("PTS").plus(col("TRB")).plus(col("AST")))
                .filter(col("efficiency").gt(0));

        Dataset<Row> cleanSalaries = salaries
                .withColumn("salary", col("salary").cast("double"))
                .filter(col("salary").isNotNull().and(col("player_id").isNotNull()))
                .select(
                        col("player_id"),
                        col("season_end").cast("int").as("year"),
                        col("salary")
                );

        Dataset<Row> yearlySalaries = cleanSalaries
                .groupBy("player_id", "year")
                .agg(sum(col("salary")).as("total_salary"));

        return yearlySalaries
                .join(yearlyStats, new String[]{"player_id", "year"}, "inner")
                .join(statsTeamCounts, new String[]{"player_id", "year"}, "inner")
                .filter(col("team_count").gt(0))
                .withColumn("cost_per_efficiency",
                        col("total_salary").divide(col("team_count")).divide(col("efficiency")))
                .select(
                        col("year"),
                        col("player_id"),
                        col("player_name"),
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
