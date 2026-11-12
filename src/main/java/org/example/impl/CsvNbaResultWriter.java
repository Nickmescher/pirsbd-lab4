package org.example.impl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.example.NbaResultWriter;

public class CsvNbaResultWriter implements NbaResultWriter {

    @Override
    public void write(Dataset<Row> data, String outputPath, String name) {
        data.coalesce(1)
                .write()
                .option("header", "true")
                .mode("overwrite")
                .csv(outputPath + "/" + name);
    }
}
