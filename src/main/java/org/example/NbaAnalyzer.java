package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface NbaAnalyzer {

    /**
     * Считает для каждого игрока за каждый сезон:
     *   efficiency = годовые PTS + TRB + AST только из строк TOT
     *   salary = сумма зарплат за сезон / число различных Tm в stats, исключая TOT
     *   cost_per_efficiency = salary / efficiency
     *
     * Принимает исходные salaries (player_id, season_end, salary, team), stats и players.
     * Сопоставляет статистику с players по имени и ближайшей дате рождения
     * к 1 мая (Year - Age), затем соединяет с зарплатами по player_id и году окончания сезона.
     */
    Dataset<Row> costPerEfficiency(Dataset<Row> salaries, Dataset<Row> stats, Dataset<Row> players);

    /**
     * Из датасета cost_per_efficiency возвращает топ-N игроков
     * с наименьшим cost_per_efficiency за каждый год.
     */
    Dataset<Row> topCheapestByYear(Dataset<Row> costPerEfficiency, int topN);
}
