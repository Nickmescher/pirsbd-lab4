package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.example.impl.CsvNbaDataLoader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.spark.sql.RowFactory.create;
import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class NbaDataCleanerTest {
    private SparkSession spark;

    @BeforeAll
    void startSpark() {
        SparkSession.Builder builder = SparkSession.builder().master("local[2]")
                .appName("nba-cleaning-test").config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "2").config("spark.driver.host", "127.0.0.1");
        if (System.getProperty("os.name").startsWith("Windows")) {
            builder.config("spark.hadoop.fs.file.impl", "com.globalmentor.apache.hadoop.fs.BareLocalFileSystem");
        }
        spark = builder.getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
    }

    @AfterAll
    void stopSpark() {
        if (spark != null) spark.stop();
    }

    private Dataset<Row> strings(String names, Row... rows) {
        String schema = Arrays.stream(names.split(","))
                .map(name -> "`" + name.trim() + "` STRING").collect(Collectors.joining(","));
        return spark.createDataFrame(Arrays.asList(rows), StructType.fromDDL(schema));
    }

    @Test
    void keepsMissingOptionalPlayerFieldsAndHomonymsButRejectsMissingIds() {
        Dataset<Row> cleaned = NbaDataCleaner.cleanPlayers(strings("index,_id,name,college,career_G,career_WS",
                create("0", " first01 ", " Shared   Name* ", "College", "100", "-0.5"),
                create("1", "first01", "Shared Name", "College", "100", "-0.5"),
                create("2", "second01", "Shared Name", "College", "100", "0"),
                create("3", "missing01", "Missing college", " ", "1", "0"),
                create("4", "bad01", "Bad number", "College", "bad", "0"),
                create("5", "", "No ID", "College", "1", "0")));
        Map<String, Row> facts = cleaned.collectAsList().stream()
                .collect(Collectors.toMap(row -> row.getAs("_id"), row -> row));
        assertEquals(Set.of("first01", "second01", "missing01", "bad01"), facts.keySet());
        assertEquals("Shared Name", facts.get("first01").getAs("name"));
        assertEquals(-0.5, (Double) facts.get("first01").getAs("career_WS"));
        assertNull(facts.get("missing01").getAs("college"));
        assertNull(facts.get("bad01").getAs("career_G"));
        assertFalse(Arrays.asList(cleaned.columns()).contains("index"));
    }

    @Test
    void rejectsInvalidRequiredSalaryFieldsAndKeepsMissingMetadataWithoutSummingTeams() {
        Dataset<Row> cleaned = NbaDataCleaner.cleanSalaries(strings(
                "index,player_id,salary,season_start,season_end,league,team,season",
                create("0", "alice01", "1000", "2014", "2015", " nba ", " A ", "2014-15"),
                create("1", "alice01", "1000", "2014", "2015", "NBA", "A", "2014-15"),
                create("2", "alice01", "300", "2014", "2015", "NBA", "B", "2014-15"),
                create("3", "alice01", "0", "2014", "2015", "NBA", "A", "2014-15"),
                create("4", "alice01", "-1", "2014", "2015", "NBA", "A", "2014-15"),
                create("5", "alice01", "bad", "2014", "2015", "NBA", "A", "2014-15"),
                create("6", "alice01", "1000", "2013", "2015", "NBA", "A", "2014-15"),
                create("7", "alice01", "1000", "2014", "2015", "ABA", "A", "2014-15"),
                create("8", "bob01", "500", "2014", "2015", "NBA", null, "2014-15"),
                create("9", "carol01", "750", null, "2015", null, null, null),
                create("10", "dave01", null, "2014", "2015", "NBA", "A", "2014-15"),
                create("11", "eve01", "1000", "2014", null, "NBA", "A", "2014-15"),
                create("12", null, "1000", "2014", "2015", "NBA", "A", "2014-15")));
        assertEquals(4, cleaned.count());
        assertEquals(Set.of(1000L, 300L, 500L, 750L), cleaned.collectAsList().stream()
                .map(row -> row.<Long>getAs("salary")).collect(Collectors.toSet()));
        assertNull(cleaned.filter("player_id = 'carol01'").first().getAs("season_start"));
        assertNull(cleaned.filter("player_id = 'bob01'").first().getAs("team"));
    }

    @Test
    void dropsMissingEfficiencyFieldsButKeepsMissingOptionalStatsAndRealZeros() {
        Dataset<Row> cleaned = NbaDataCleaner.cleanStats(strings(
                "_c0,Year,Player,Pos,Age,Tm,G,PTS,TRB,AST,WS,blank2",
                create("0", "2015", " Alice* ", "PG", "25", "TOT", "20", "0", "0", "0", "-0.5", ""),
                create("1", "2015", "Alice", "PG", "25", "TOT", "20", "0", "0", "0", "-0.5", ""),
                create("2", "2015", "Alice", "PG", "25", "AAA", "10", "5", "0", "0", "0", ""),
                create("3", "2015", "Alice", "PG", "25", "BBB", "10", "0", "0", "0", "0", ""),
                create("4", "2016", "Null points", "PG", "26", "AAA", "1", null, "0", "0", "0", ""),
                create("5", "2016", "Empty points", "PG", "26", "AAA", "1", " ", "0", "0", "0", ""),
                create("6", "2016", "Null rebounds", "PG", "26", "AAA", "1", "1", null, "0", "0", ""),
                create("7", "2016", "Null assists", "PG", "26", "AAA", "1", "1", "0", null, "0", ""),
                create("8", "2016", "Bad points", "PG", "26", "AAA", "1", "bad", "0", "0", "0", ""),
                create("9", "2016", "Negative points", "PG", "26", "AAA", "1", "-1", "0", "0", "0", ""),
                create("10", "", "", "", "", "", "", "", "", "", "", ""),
                create("11", "2016", "Optional fields missing", null, "bad", null, null, "1", "0", "0", "-", ""),
                create("12", "2016", "Negative rebounds", "PG", "26", "AAA", "1", "1", "-1", "0", "0", ""),
                create("13", "2016", "Negative assists", "PG", "26", "AAA", "1", "1", "0", "-1", "0", ""),
                create("14", null, "Missing year", "PG", "26", "AAA", "1", "1", "0", "0", "0", ""),
                create("15", "2016", " ", "PG", "26", "AAA", "1", "1", "0", "0", "0", "")));
        Map<String, Row> facts = cleaned.collectAsList().stream()
                .collect(Collectors.toMap(row -> row.getAs("Tm"), row -> row));
        assertEquals(4, facts.size());
        assertTrue(facts.keySet().containsAll(Set.of("TOT", "AAA", "BBB")));
        assertEquals("Optional fields missing", facts.get(null).getAs("Player"));
        assertNull(facts.get(null).getAs("Age"));
        assertNull(facts.get(null).getAs("WS"));
        assertEquals(0, (Integer) facts.get("TOT").getAs("PTS"));
        assertEquals(-0.5, (Double) facts.get("TOT").getAs("WS"));
        assertFalse(Arrays.asList(cleaned.columns()).contains("blank2"));
        assertFalse(Arrays.asList(cleaned.columns()).contains("efficiency"));
        assertFalse(cleaned.queryExecution().optimizedPlan().toString().contains("Join"));
    }

    @Test
    void dataLoaderAppliesCleaningToAllThreeSources(@TempDir Path temp) throws Exception {
        Files.writeString(temp.resolve("players.csv"), "index,_id,name,position,birthDate\n"
                + "0,alice01,Alice,Guard,\"January 1, 1990\"\n1,,No ID,Guard,\"January 1, 1990\"\n");
        Files.writeString(temp.resolve("salaries.csv"),
                "index,league,player_id,salary,season,season_end,season_start,team\n"
                + "0,NBA,alice01,1000,2014-15,2015,2014,Team A\n"
                + "1,NBA,alice01,0,2014-15,2015,2014,Team A\n");
        Files.writeString(temp.resolve("stats.csv"), ",Year,Player,Pos,Age,Tm,G,PTS,TRB,AST\n"
                + "0,2015,Alice*,PG,25,TOT,20,0,30,0\n1,2015,Alice,PG,25,AAA,1,,0,0\n");
        NbaDataLoader loader = new CsvNbaDataLoader();
        assertEquals(1, loader.loadPlayers(spark, temp.resolve("players.csv").toString()).count());
        assertEquals(1, loader.loadSalaries(spark, temp.resolve("salaries.csv").toString()).count());
        Dataset<Row> stats = loader.loadSeasonStats(spark, temp.resolve("stats.csv").toString());
        assertEquals(1, stats.count());
        assertEquals("Alice", stats.first().getAs("Player"));
        assertEquals(0, (Integer) stats.first().getAs("PTS"));
    }
}
