package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.example.impl.CsvNbaDataLoader;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.*;

public final class NbaDataCleaner {
    private static final List<String> EXPORT_COLUMNS = List.of("index", "_c0", "blanl", "blank2");
    private static final List<String> STAT_COUNTS = List.of(
            "G", "GS", "MP", "FG", "FGA", "3P", "3PA", "2P", "2PA", "FT", "FTA",
            "ORB", "DRB", "TRB", "AST", "STL", "BLK", "TOV", "PF", "PTS");

    private NbaDataCleaner() {
    }

    private static Dataset<Row> normalize(Dataset<Row> raw) {
        Column[] columns = Arrays.stream(raw.columns())
                .filter(name -> !EXPORT_COLUMNS.contains(name))
                .map(name -> {
                    Column value = trim(col("`" + name + "`").cast("string"));
                    return when(length(value).gt(0), value).as(name);
                }).toArray(Column[]::new);
        return raw.select(columns);
    }

    private static Dataset<Row> cleanName(Dataset<Row> data, String name) {
        return data.withColumn(name,
                trim(regexp_replace(regexp_replace(col(name), "\\*+$", ""), "\\s+", " ")));
    }

    private static Dataset<Row> castNumbers(Dataset<Row> data, Map<String, String> types) {
        List<String> columns = Arrays.asList(data.columns());
        for (Map.Entry<String, String> entry : types.entrySet()) {
            if (columns.contains(entry.getKey())) {
                data = data.withColumn(entry.getKey(),
                        expr("try_cast(`" + entry.getKey() + "` as " + entry.getValue() + ")"));
            }
        }
        return data;
    }

    private static Column nonblank(String name) {
        return col(name).isNotNull().and(length(col(name)).gt(0));
    }

    public static Dataset<Row> cleanPlayers(Dataset<Row> raw) {
        Dataset<Row> data = cleanName(normalize(raw), "name");
        Map<String, String> types = new LinkedHashMap<>();
        for (String name : data.columns()) {
            if (name.startsWith("career_")) {
                types.put(name, name.equals("career_G") ? "int" : "double");
            }
        }
        return castNumbers(data, types)
                .filter(nonblank("_id").and(nonblank("name")))
                .na().drop(new String[]{"_id", "name"})
                .dropDuplicates();
    }

    public static Dataset<Row> cleanSalaries(Dataset<Row> raw) {
        Dataset<Row> data = castNumbers(normalize(raw), Map.of(
                "salary", "bigint", "season_start", "int", "season_end", "int"))
                .withColumn("league", upper(col("league")));
        // Для расчёта нужны ID, зарплата и год окончания сезона; пропуски в остальных полях допустимы.
        Column valid = nonblank("player_id")
                .and(col("league").isNull().or(col("league").equalTo("NBA")))
                .and(col("salary").gt(0)).and(col("season_end").between(1900, 2100))
                .and(col("season_start").isNull().or(col("season_start").equalTo(col("season_end").minus(1))));
        return data.filter(valid).na().drop(new String[]{"player_id", "salary", "season_end"}).dropDuplicates();
    }

    public static Dataset<Row> cleanStats(Dataset<Row> raw) {
        Dataset<Row> data = cleanName(normalize(raw), "Player");
        Map<String, String> types = new LinkedHashMap<>();
        for (String name : data.columns()) {
            if (!List.of("Player", "Pos", "Tm").contains(name)) {
                types.put(name, STAT_COUNTS.contains(name) || name.equals("Year") || name.equals("Age")
                        ? "int" : "double");
            }
        }
        data = castNumbers(data, types);
        Column valid = col("Year").between(1900, 2100).and(nonblank("Player"));
        // Пропуски и отрицательные значения исключаем только у слагаемых эффективности.
        for (String name : List.of("PTS", "TRB", "AST")) {
            valid = valid.and(col(name).geq(0));
        }
        return data.filter(valid).na().drop(new String[]{"Year", "Player", "PTS", "TRB", "AST"}).dropDuplicates();
    }

    public static void main(String[] args) {
        if (args.length > 2) {
            throw new IllegalArgumentException("Usage: NbaDataCleaner [inputDir] [outputDir]");
        }
        String input = args.length > 0 ? args[0] : "data";
        String output = args.length > 1 ? args[1] : "output/cleaned";
        SparkConf conf = new SparkConf().setAppName("nba-csv-cleaning");
        if (!conf.contains("spark.master")) {
            conf.setMaster("local[2]");
        }
        SparkSession.Builder builder = SparkSession.builder().config(conf)
                .config("spark.sql.shuffle.partitions", "4");
        if (System.getProperty("os.name").startsWith("Windows")) {
            builder.config("spark.hadoop.fs.file.impl", "com.globalmentor.apache.hadoop.fs.BareLocalFileSystem");
        }
        SparkSession spark = builder.getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        try {
            NbaDataLoader loader = new CsvNbaDataLoader();
            write(loader.loadPlayers(spark, input + "/players.csv"), output + "/players");
            write(loader.loadSalaries(spark, input + "/salaries_1985to2018.csv"), output + "/salaries");
            write(loader.loadSeasonStats(spark, input + "/Seasons_Stats.csv"), output + "/seasons_stats");
        } finally {
            spark.stop();
        }
    }

    private static void write(Dataset<Row> data, String path) {
        data.coalesce(1).write().mode("overwrite").option("header", "true")
                .option("encoding", "UTF-8").option("escape", "\"").csv(path);
        System.out.println("Cleaned CSV: " + path + " rows=" + data.count());
    }
}
