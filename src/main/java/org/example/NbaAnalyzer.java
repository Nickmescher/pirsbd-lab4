package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface NbaAnalyzer {

    /**
     * Считает для каждого игрока за каждый сезон:
     *   efficiency = PTS + TRB + AST
     *   cost_per_efficiency = salary / efficiency
     *
     * Принимает salaries уже обогащённый колонками player_name и year.
     */
    Dataset<Row> costPerEfficiency(Dataset<Row> salaries, Dataset<Row> stats);

    /**
     * Из датасета cost_per_efficiency возвращает топ-N игроков
     * с наименьшим cost_per_efficiency за каждый год.
     */
    Dataset<Row> topCheapestByYear(Dataset<Row> costPerEfficiency, int topN);
}
