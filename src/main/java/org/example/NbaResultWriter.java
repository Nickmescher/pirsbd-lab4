package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface NbaResultWriter {
    void write(Dataset<Row> data, String outputPath, String name);
}
