package com.usaccidents.hive;

import com.usaccidents.io.HiveUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class USAccidentsHiveDataProcessor {
    private static final Logger logger = LoggerFactory.getLogger(USAccidentsHiveDataProcessor.class);
    private final HiveUtils hiveUtils;
    private final String rawTableName;

    public USAccidentsHiveDataProcessor(HiveUtils hiveUtils, String rawTableName) {
        this.hiveUtils = hiveUtils;
        this.rawTableName = rawTableName;
    }

    /**
     * Execute the complete Hive data processing workflow
     */
    public void executeWorkflow(List<String> hdfsFilePaths) {
        try {
            // Step 1: Create the raw table
            createRawAccidentsTable();

            // Step 2: Load data from CSV files in HDFS
            for (String hdfsFilePath : hdfsFilePaths) {
                loadDataFromHDFS(hdfsFilePath);
            }

            // Step 3: Create analysis tables
            createAnalysisTables();

            // Step 4: Populate analysis tables
            populateLocationAnalysisTable();
            populateSeverityAnalysisTable();
            populateTimeAnalysisTable();
            populateWeatherAnalysisTable();

            // Step 5: Verify data was loaded correctly
            verifyTableCounts();

            logger.info("Hive data processing workflow completed successfully");
        } catch (Exception e) {
            logger.error("Error in Hive data processing workflow: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to process Hive data", e);
        }
    }

    /**
     * Modified workflow that skips data loading
     */
    public void executeAnalysisOnly() {
        try {
            // Step 1: Verify raw table exists and has data
            verifyRawTable();

            // Step 2: Create analysis tables
            createAnalysisTables();

            // Step 3: Populate analysis tables
            populateLocationAnalysisTable();
            populateSeverityAnalysisTable();
            populateTimeAnalysisTable();
            populateWeatherAnalysisTable();

            // Step 4: Verify data was processed correctly
            verifyTableCounts();

            logger.info("Hive data analysis workflow completed successfully");
        } catch (Exception e) {
            logger.error("Error in Hive data analysis workflow: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to process Hive data", e);
        }
    }

    /**
     * Verify the raw table exists and has data
     */
    private void verifyRawTable() {
        logger.info("Verifying raw table: {}", rawTableName);

        List<Map<String, Object>> tables = hiveUtils.executeQuery(
                "SHOW TABLES LIKE '" + rawTableName + "'");

        if (tables.isEmpty()) {
            throw new RuntimeException("Raw table " + rawTableName + " does not exist");
        }

        List<Map<String, Object>> count = hiveUtils.executeQuery(
                "SELECT COUNT(*) as cnt FROM " + rawTableName);

        long rowCount = (long) count.get(0).get("cnt");
        if (rowCount == 0) {
            throw new RuntimeException("Raw table " + rawTableName + " is empty");
        }

        logger.info("Raw table verified with {} rows", rowCount);
    }

    /**
     * Create the raw accidents table in Hive with STRING types for timestamps and booleans
     */
    private void createRawAccidentsTable() {
        logger.info("Creating raw accidents table: {}", rawTableName);

        String createTableSQL =
                "CREATE TABLE IF NOT EXISTS " + rawTableName + " (" +
                        "Source STRING, " +
                        "Severity INT, " +
                        "Start_Time STRING, " +
                        "End_Time STRING, " +
                        "Start_Lat DOUBLE, " +
                        "Start_Lng DOUBLE, " +
                        "Distance_mi DOUBLE, " +
                        "Description STRING, " +
                        "Street STRING, " +
                        "City STRING, " +
                        "County STRING, " +
                        "State STRING, " +
                        "Zipcode STRING, " +
                        "Country STRING, " +
                        "Timezone STRING, " +
                        "Airport_Code STRING, " +
                        "Temperature_F DOUBLE, " +
                        "Wind_Chill_F DOUBLE, " +
                        "Humidity_percent DOUBLE, " +
                        "Pressure_in DOUBLE, " +
                        "Visibility_mi DOUBLE, " +
                        "Wind_Direction STRING, " +
                        "Wind_Speed_mph DOUBLE, " +
                        "Precipitation_in DOUBLE, " +
                        "Weather_Condition STRING, " +
                        "Amenity STRING, " +
                        "Bump STRING, " +
                        "Crossing STRING, " +
                        "Give_Way STRING, " +
                        "Junction STRING, " +
                        "No_Exit STRING, " +
                        "Railway STRING, " +
                        "Roundabout STRING, " +
                        "Station STRING, " +
                        "Stop STRING, " +
                        "Traffic_Calming STRING, " +
                        "Traffic_Signal STRING, " +
                        "Turning_Loop STRING, " +
                        "Sunrise_Sunset STRING, " +
                        "Civil_Twilight STRING, " +
                        "Nautical_Twilight STRING, " +
                        "Astronomical_Twilight STRING, " +
                        "ID STRING, " +
                        "Weather_Timestamp_Filled STRING" +
                        ") " +
                        "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' " +
                        "STORED AS TEXTFILE " +
                        "TBLPROPERTIES ( " +
                        "   'skip.header.line.count'='1', " +
                        "   'serialization.null.format'=''" +
                        ")";

        hiveUtils.executeUpdate(createTableSQL);
        logger.info("Raw accidents table created successfully");
    }

    /**
     * Load data from HDFS into the raw table
     */
    private void loadDataFromHDFS(String hdfsFilePath) {
        logger.info("Loading data from HDFS file: {} into table: {}", hdfsFilePath, rawTableName);
        String loadDataSQL = "LOAD DATA INPATH '" + hdfsFilePath + "' INTO TABLE " + rawTableName;
        hiveUtils.executeUpdate(loadDataSQL);
        logger.info("Data loaded successfully from: {}", hdfsFilePath);
    }

    /**
     * Create the analysis tables in Hive
     */
    private void createAnalysisTables() {
        logger.info("Creating analysis tables");

        // Create location_analysis table
        String createLocationTableSQL =
                "CREATE TABLE IF NOT EXISTS location_analysis (" +
                        "accident_id STRING, " +
                        "state STRING, " +
                        "city STRING, " +
                        "county STRING, " +
                        "zipcode STRING, " +
                        "latitude DOUBLE, " +
                        "longitude DOUBLE, " +
                        "street_type STRING, " +
                        "has_amenity STRING, " +
                        "has_bump STRING, " +
                        "has_crossing STRING, " +
                        "has_junction STRING, " +
                        "has_traffic_signal STRING, " +
                        "accident_count INT" +
                        ") " +
                        "STORED AS ORC " +
                        "TBLPROPERTIES (\"orc.compress\"=\"SNAPPY\")";

        hiveUtils.executeUpdate(createLocationTableSQL);

        // Create severity_analysis table
        String createSeverityTableSQL =
                "CREATE TABLE IF NOT EXISTS severity_analysis (" +
                        "severity_level INT, " +
                        "severity_description STRING, " +
                        "common_time_of_day STRING, " +
                        "common_weather_condition STRING, " +
                        "count_by_severity INT" +
                        ") " +
                        "STORED AS ORC " +
                        "TBLPROPERTIES (\"orc.compress\"=\"SNAPPY\")";

        hiveUtils.executeUpdate(createSeverityTableSQL);

        // Create time_analysis table
        String createTimeTableSQL =
                "CREATE TABLE IF NOT EXISTS time_analysis (" +
                        "hour_of_day INT, " +
                        "day_of_week STRING, " +
                        "month_of_year STRING, " +
                        "year INT, " +
                        "sunrise_sunset_period STRING, " +
                        "twilight_period STRING, " +
                        "accident_count INT" +
                        ") " +
                        "STORED AS ORC " +
                        "TBLPROPERTIES (\"orc.compress\"=\"SNAPPY\")";

        hiveUtils.executeUpdate(createTimeTableSQL);

        // Create weather_analysis table
        String createWeatherTableSQL =
                "CREATE TABLE IF NOT EXISTS weather_analysis (" +
                        "weather_condition STRING, " +
                        "temperature_range STRING, " +
                        "visibility_range STRING, " +
                        "precipitation_level STRING, " +
                        "wind_speed_range STRING, " +
                        "average_severity DOUBLE, " +
                        "accident_count INT" +
                        ") " +
                        "STORED AS ORC " +
                        "TBLPROPERTIES (\"orc.compress\"=\"SNAPPY\")";

        hiveUtils.executeUpdate(createWeatherTableSQL);

        logger.info("All analysis tables created successfully");
    }

    /**
     * Populate the location_analysis table with your specific query
     */
    private void populateLocationAnalysisTable() {
        logger.info("Populating location_analysis table");

        // First, clear the table if it has data
        hiveUtils.executeUpdate("TRUNCATE TABLE location_analysis");

        String insertLocationSQL =
                "INSERT INTO TABLE location_analysis " +
                        "SELECT " +
                        "    ID as accident_id, " +
                        "    State, " +
                        "    City, " +
                        "    County, " +
                        "    Zipcode, " +
                        "    Start_Lat as latitude, " +
                        "    Start_Lng as longitude, " +
                        "    CASE " +
                        "        WHEN Street LIKE '%I-%' THEN 'Interstate' " +
                        "        WHEN Street LIKE '%US-%' THEN 'US Highway' " +
                        "        WHEN Street LIKE '%State Route%' THEN 'State Highway' " +
                        "        WHEN Street LIKE '%Rd%' THEN 'Road' " +
                        "        WHEN Street LIKE '%Ave%' THEN 'Avenue' " +
                        "        WHEN Street LIKE '%St%' THEN 'Street' " +
                        "        ELSE 'Other' " +
                        "    END as street_type, " +
                        "    Amenity as has_amenity, " +
                        "    Bump as has_bump, " +
                        "    Crossing as has_crossing, " +
                        "    Junction as has_junction, " +
                        "    Traffic_Signal as has_traffic_signal, " +
                        "    1 as accident_count " +
                        "FROM " + rawTableName;

        hiveUtils.executeUpdate(insertLocationSQL);
        logger.info("Location analysis table populated");
    }

    /**
     * Populate the severity_analysis table with your specific query
     */
    private void populateSeverityAnalysisTable() {
        logger.info("Populating severity_analysis table");

        // First, clear the table if it has data
        hiveUtils.executeUpdate("TRUNCATE TABLE severity_analysis");

        String insertSeveritySQL =
                "INSERT INTO TABLE severity_analysis " +
                        "SELECT " +
                        "    Severity as severity_level, " +
                        "    CASE " +
                        "        WHEN Severity = 1 THEN 'Low' " +
                        "        WHEN Severity = 2 THEN 'Moderate' " +
                        "        WHEN Severity = 3 THEN 'High' " +
                        "        WHEN Severity = 4 THEN 'Very High' " +
                        "        ELSE 'Unknown' " +
                        "    END as severity_description, " +
                        "    CASE " +
                        "        WHEN hour(from_unixtime(unix_timestamp(Start_Time))) BETWEEN 6 AND 11 THEN 'Morning' " +
                        "        WHEN hour(from_unixtime(unix_timestamp(Start_Time))) BETWEEN 12 AND 17 THEN 'Afternoon' " +
                        "        WHEN hour(from_unixtime(unix_timestamp(Start_Time))) BETWEEN 18 AND 23 THEN 'Evening' " +
                        "        ELSE 'Night' " +
                        "    END as common_time_of_day, " +
                        "    Weather_Condition as common_weather_condition, " +
                        "    COUNT(*) as count_by_severity " +
                        "FROM " + rawTableName + " " +
                        "GROUP BY " +
                        "    Severity, " +
                        "    CASE " +
                        "        WHEN Severity = 1 THEN 'Low' " +
                        "        WHEN Severity = 2 THEN 'Moderate' " +
                        "        WHEN Severity = 3 THEN 'High' " +
                        "        WHEN Severity = 4 THEN 'Very High' " +
                        "        ELSE 'Unknown' " +
                        "    END, " +
                        "    CASE " +
                        "        WHEN hour(from_unixtime(unix_timestamp(Start_Time))) BETWEEN 6 AND 11 THEN 'Morning' " +
                        "        WHEN hour(from_unixtime(unix_timestamp(Start_Time))) BETWEEN 12 AND 17 THEN 'Afternoon' " +
                        "        WHEN hour(from_unixtime(unix_timestamp(Start_Time))) BETWEEN 18 AND 23 THEN 'Evening' " +
                        "        ELSE 'Night' " +
                        "    END, " +
                        "    Weather_Condition";

        hiveUtils.executeUpdate(insertSeveritySQL);
        logger.info("Severity analysis table populated");
    }

    /**
     * Populate the time_analysis table with your specific query
     */
    private void populateTimeAnalysisTable() {
        logger.info("Populating time_analysis table");

        // First, clear the table if it has data
        hiveUtils.executeUpdate("TRUNCATE TABLE time_analysis");

        String insertTimeSQL =
                "INSERT INTO TABLE time_analysis " +
                        "SELECT " +
                        "    hour(from_unixtime(unix_timestamp(regexp_replace(Start_Time, '\"', ''), 'yyyy-MM-dd HH:mm:ss'))) as hour_of_day, " +
                        "    date_format(from_unixtime(unix_timestamp(regexp_replace(Start_Time, '\"', ''), 'yyyy-MM-dd HH:mm:ss')), 'EEEE') as day_of_week, " +
                        "    date_format(from_unixtime(unix_timestamp(regexp_replace(Start_Time, '\"', ''), 'yyyy-MM-dd HH:mm:ss')), 'MMMM') as month_of_year, " +
                        "    year(from_unixtime(unix_timestamp(regexp_replace(Start_Time, '\"', ''), 'yyyy-MM-dd HH:mm:ss'))) as year, " +
                        "    Sunrise_Sunset as sunrise_sunset_period, " +
                        "    CASE " +
                        "        WHEN Civil_Twilight = 'Day' THEN 'Day' " +
                        "        WHEN Nautical_Twilight = 'Day' THEN 'Dawn/Dusk' " +
                        "        WHEN Astronomical_Twilight = 'Day' THEN 'Night' " +
                        "        ELSE 'Night' " +
                        "    END as twilight_period, " +
                        "    COUNT(*) as accident_count " +
                        "FROM " + rawTableName + " " +
                        "GROUP BY " +
                        "    hour(from_unixtime(unix_timestamp(regexp_replace(Start_Time, '\"', ''), 'yyyy-MM-dd HH:mm:ss'))), " +
                        "    date_format(from_unixtime(unix_timestamp(regexp_replace(Start_Time, '\"', ''), 'yyyy-MM-dd HH:mm:ss')), 'EEEE'), " +
                        "    date_format(from_unixtime(unix_timestamp(regexp_replace(Start_Time, '\"', ''), 'yyyy-MM-dd HH:mm:ss')), 'MMMM'), " +
                        "    year(from_unixtime(unix_timestamp(regexp_replace(Start_Time, '\"', ''), 'yyyy-MM-dd HH:mm:ss'))), " +
                        "    Sunrise_Sunset, " +
                        "    CASE " +
                        "        WHEN Civil_Twilight = 'Day' THEN 'Day' " +
                        "        WHEN Nautical_Twilight = 'Day' THEN 'Dawn/Dusk' " +
                        "        WHEN Astronomical_Twilight = 'Day' THEN 'Night' " +
                        "        ELSE 'Night' " +
                        "    END";


        hiveUtils.executeUpdate(insertTimeSQL);
        logger.info("Time analysis table populated");
    }

    /**
     * Populate the weather_analysis table with your specific query
     */
    private void populateWeatherAnalysisTable() {
        logger.info("Populating weather_analysis table");

        // First, clear the table if it has data
        hiveUtils.executeUpdate("TRUNCATE TABLE weather_analysis");

        String insertWeatherSQL =
                "INSERT INTO TABLE weather_analysis " +
                        "SELECT " +
                        "    weather_condition, " +
                        "    temperature_range, " +
                        "    visibility_range, " +
                        "    precipitation_level, " +
                        "    wind_speed_range, " +
                        "    AVG(Severity) as average_severity, " +
                        "    COUNT(*) as accident_count " +
                        "FROM ( " +
                        "    SELECT " +
                        "        Weather_Condition AS weather_condition, " +
                        "        CASE " +
                        "            WHEN Temperature_F < 32 THEN 'Below Freezing' " +
                        "            WHEN Temperature_F BETWEEN 32 AND 50 THEN 'Cold' " +
                        "            WHEN Temperature_F BETWEEN 50 AND 70 THEN 'Mild' " +
                        "            WHEN Temperature_F BETWEEN 70 AND 85 THEN 'Warm' " +
                        "            ELSE 'Hot' " +
                        "        END AS temperature_range, " +
                        "        CASE " +
                        "            WHEN Visibility_mi < 1 THEN 'Very Low' " +
                        "            WHEN Visibility_mi BETWEEN 1 AND 3 THEN 'Low' " +
                        "            WHEN Visibility_mi BETWEEN 3 AND 6 THEN 'Moderate' " +
                        "            WHEN Visibility_mi BETWEEN 6 AND 10 THEN 'Good' " +
                        "            ELSE 'Excellent' " +
                        "        END AS visibility_range, " +
                        "        CASE " +
                        "            WHEN Precipitation_in = 0 THEN 'None' " +
                        "            WHEN Precipitation_in BETWEEN 0 AND 0.1 THEN 'Light' " +
                        "            WHEN Precipitation_in BETWEEN 0.1 AND 0.3 THEN 'Moderate' " +
                        "            ELSE 'Heavy' " +
                        "        END AS precipitation_level, " +
                        "        CASE " +
                        "            WHEN Wind_Speed_mph = 0 THEN 'Calm' " +
                        "            WHEN Wind_Speed_mph BETWEEN 1 AND 7 THEN 'Light' " +
                        "            WHEN Wind_Speed_mph BETWEEN 8 AND 25 THEN 'Moderate' " +
                        "            WHEN Wind_Speed_mph BETWEEN 26 AND 54 THEN 'Strong' " +
                        "            ELSE 'Violent' " +
                        "        END AS wind_speed_range, " +
                        "        Severity " +
                        "    FROM " + rawTableName + " " +
                        "    WHERE Weather_Condition IS NOT NULL " +
                        ") tmp " +
                        "GROUP BY " +
                        "    weather_condition, " +
                        "    temperature_range, " +
                        "    visibility_range, " +
                        "    precipitation_level, " +
                        "    wind_speed_range";

        hiveUtils.executeUpdate(insertWeatherSQL);
        logger.info("Weather analysis table populated");
    }

    /**
     * Verify that tables were populated with data
     */
    private void verifyTableCounts() {
        // Get counts for each table
        List<Map<String, Object>> rawCount = hiveUtils.executeQuery("SELECT COUNT(*) as count FROM " + rawTableName);
        List<Map<String, Object>> locationCount = hiveUtils.executeQuery("SELECT COUNT(*) as count FROM location_analysis");
        List<Map<String, Object>> severityCount = hiveUtils.executeQuery("SELECT COUNT(*) as count FROM severity_analysis");
        List<Map<String, Object>> timeCount = hiveUtils.executeQuery("SELECT COUNT(*) as count FROM time_analysis");
        List<Map<String, Object>> weatherCount = hiveUtils.executeQuery("SELECT COUNT(*) as count FROM weather_analysis");

        // Log table counts
        logger.info("Table row counts:");
        logger.info("  Raw accidents table: {}", rawCount.get(0).get("count"));
        logger.info("  Location analysis table: {}", locationCount.get(0).get("count"));
        logger.info("  Severity analysis table: {}", severityCount.get(0).get("count"));
        logger.info("  Time analysis table: {}", timeCount.get(0).get("count"));
        logger.info("  Weather analysis table: {}", weatherCount.get(0).get("count"));
    }


    /**
     * Selects and displays 5 sample rows from a specified table
     * @param tableName The name of the table to query
     */
    public void selectSampleRows(String tableName) {
        logger.info("Fetching sample rows from table: {}", tableName);

        try {
            String query = "SELECT * FROM " + tableName + " LIMIT 5";
            List<Map<String, Object>> results = hiveUtils.executeQuery(query);

            if (results.isEmpty()) {
                logger.info("No data found in table: {}", tableName);
                return;
            }

            // Print header
            System.out.println("\nSample rows from table: " + tableName);
            System.out.println("----------------------------------------");

            // Print column names
            System.out.println(String.join(" | ", results.get(0).keySet()));

            // Print rows
            for (Map<String, Object> row : results) {
                System.out.println(String.join(" | ",
                        row.values().stream()
                                .map(Object::toString)
                                .toArray(String[]::new)
                ));
            }

        } catch (Exception e) {
            logger.error("Error selecting sample rows from {}: {}", tableName, e.getMessage());
            System.err.println("Error selecting sample rows from " + tableName + ": " + e.getMessage());
        }
    }

    /**
     * Selects and displays 5 sample rows from all analysis tables and the raw table
     */
    public void selectSampleFromAllTables() {
        String[] tables = {
                rawTableName,  // Use the instance variable for raw table name
                "location_analysis",
                "severity_analysis",
                "time_analysis",
                "weather_analysis"
        };

        for (String table : tables) {
            try {
                selectSampleRows(table);
            } catch (Exception e) {
                logger.error("Error sampling table {}: {}", table, e.getMessage());
            }
        }
    }

    /**
     * Saves the contents of a Hive table to a CSV file
     * @param tableName The name of the table to export
     * @param outputPath The full path to the output CSV file
     */
    public void saveTableToCSV(String tableName, String outputPath) throws IOException {
        logger.info("Exporting {} table to CSV file: {}", tableName, outputPath);

        // Get all data from the table first
        String query = "SELECT * FROM " + tableName;
        List<Map<String, Object>> results = hiveUtils.executeQuery(query);

        if (results.isEmpty()) {
            logger.warn("Table {} is empty, creating empty CSV file", tableName);
            // Create empty file
            try (FileWriter writer = new FileWriter(outputPath)) {
                writer.write("");
            }
            return;
        }

        try (FileWriter writer = new FileWriter(outputPath)) {
            // Write header row
            writer.write(String.join(",", results.get(0).keySet()) + "\n");

            // Write data rows
            for (Map<String, Object> row : results) {
                List<String> values = new ArrayList<>();
                for (Object value : row.values()) {
                    String strValue = (value != null) ? value.toString() : "";
                    // Escape commas in values
                    if (strValue.contains(",")) {
                        strValue = "\"" + strValue + "\"";
                    }
                    values.add(strValue);
                }
                writer.write(String.join(",", values) + "\n");
            }
        }
        logger.info("Successfully exported {} rows from {} to {}", results.size(), tableName, outputPath);
    }

    /**
     * Saves all analysis tables to CSV files in the specified directory
     * @param outputDir The directory where CSV files should be saved
     */
    public void saveAllAnalysisTablesToCSV(String outputDir) {
        String[] analysisTables = {
                "location_analysis",
                "severity_analysis",
                "time_analysis",
                "weather_analysis"
        };

        // Ensure directory exists
        File dir = new File(outputDir);
        if (!dir.exists()) {
            dir.mkdirs();
        }

        for (String table : analysisTables) {
            String outputPath = outputDir + File.separator + table + ".csv";
            try {
                saveTableToCSV(table, outputPath);
            } catch (Exception e) {
                logger.error("Failed to export {} table to CSV: {}", table, e.getMessage());
            }
        }
    }

}