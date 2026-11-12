package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface NbaDataCleaner {
    Dataset<Row> cleanPlayers(Dataset<Row> players);
    Dataset<Row> cleanSalaries(Dataset<Row> salaries);
    Dataset<Row> cleanStats(Dataset<Row> stats);
}
