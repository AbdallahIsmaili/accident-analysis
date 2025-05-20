package com.usaccidents.hive;

import com.usaccidents.io.HiveUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * This class handles the Hive data processing workflow for US Accidents
 * - Creates raw data table
 * - Loads data from HDFS
 * - Creates analysis tables
 * - Processes and aggregates data into analytical views
 */
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
     * Create the raw accidents table in Hive
     */
    private void createRawAccidentsTable() {
        logger.info("Creating raw accidents table: {}", rawTableName);

        String createTableSQL =
                "CREATE TABLE IF NOT EXISTS " + rawTableName + " (" +
                        "id STRING, " +
                        "severity INT, " +
                        "start_time TIMESTAMP, " +
                        "end_time TIMESTAMP, " +
                        "start_lat DOUBLE, " +
                        "start_lng DOUBLE, " +
                        "end_lat DOUBLE, " +
                        "end_lng DOUBLE, " +
                        "distance DOUBLE, " +
                        "description STRING, " +
                        "street STRING, " +
                        "city STRING, " +
                        "county STRING, " +
                        "state STRING, " +
                        "zipcode STRING, " +
                        "timezone STRING, " +
                        "weather_timestamp TIMESTAMP, " +
                        "temperature DOUBLE, " +
                        "wind_chill DOUBLE, " +
                        "humidity DOUBLE, " +
                        "pressure DOUBLE, " +
                        "visibility DOUBLE, " +
                        "wind_direction STRING, " +
                        "wind_speed DOUBLE, " +
                        "precipitation DOUBLE, " +
                        "weather_condition STRING, " +
                        "amenity BOOLEAN, " +
                        "bump BOOLEAN, " +
                        "crossing BOOLEAN, " +
                        "give_way BOOLEAN, " +
                        "junction BOOLEAN, " +
                        "no_exit BOOLEAN, " +
                        "railway BOOLEAN, " +
                        "roundabout BOOLEAN, " +
                        "station BOOLEAN, " +
                        "stop BOOLEAN, " +
                        "traffic_calming BOOLEAN, " +
                        "traffic_signal BOOLEAN, " +
                        "turning_loop BOOLEAN, " +
                        "sunrise_sunset STRING, " +
                        "civil_twilight STRING, " +
                        "nautical_twilight STRING, " +
                        "astronomical_twilight STRING" +
                        ") " +
                        "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' " +
                        "TBLPROPERTIES ('skip.header.line.count'='1')";

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
                        "has_amenity BOOLEAN, " +
                        "has_bump BOOLEAN, " +
                        "has_crossing BOOLEAN, " +
                        "has_junction BOOLEAN, " +
                        "has_traffic_signal BOOLEAN, " +
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
     * Populate the location_analysis table
     */
    private void populateLocationAnalysisTable() {
        logger.info("Populating location_analysis table");

        // First, clear the table if it has data
        hiveUtils.executeUpdate("TRUNCATE TABLE location_analysis");

        // Extract street type from the street field
        String insertLocationSQL =
                "INSERT INTO location_analysis " +
                        "SELECT " +
                        "  id as accident_id, " +
                        "  state, " +
                        "  city, " +
                        "  county, " +
                        "  zipcode, " +
                        "  start_lat as latitude, " +
                        "  start_lng as longitude, " +
                        "  CASE " +
                        "    WHEN UPPER(street) LIKE '%STREET' THEN 'Street' " +
                        "    WHEN UPPER(street) LIKE '%AVE%' OR UPPER(street) LIKE '%AVENUE%' THEN 'Avenue' " +
                        "    WHEN UPPER(street) LIKE '%BLVD%' OR UPPER(street) LIKE '%BOULEVARD%' THEN 'Boulevard' " +
                        "    WHEN UPPER(street) LIKE '%RD%' OR UPPER(street) LIKE '%ROAD%' THEN 'Road' " +
                        "    WHEN UPPER(street) LIKE '%LANE%' OR UPPER(street) LIKE '%LN%' THEN 'Lane' " +
                        "    WHEN UPPER(street) LIKE '%DR%' OR UPPER(street) LIKE '%DRIVE%' THEN 'Drive' " +
                        "    WHEN UPPER(street) LIKE '%HWY%' OR UPPER(street) LIKE '%HIGHWAY%' THEN 'Highway' " +
                        "    WHEN UPPER(street) LIKE '%PKWY%' OR UPPER(street) LIKE '%PARKWAY%' THEN 'Parkway' " +
                        "    WHEN UPPER(street) LIKE '%I-%' THEN 'Interstate' " +
                        "    ELSE 'Other' " +
                        "  END as street_type, " +
                        "  amenity as has_amenity, " +
                        "  bump as has_bump, " +
                        "  crossing as has_crossing, " +
                        "  junction as has_junction, " +
                        "  traffic_signal as has_traffic_signal, " +
                        "  1 as accident_count " +
                        "FROM " + rawTableName;

        hiveUtils.executeUpdate(insertLocationSQL);
        logger.info("Location analysis table populated");
    }

    /**
     * Populate the severity_analysis table
     */
    private void populateSeverityAnalysisTable() {
        logger.info("Populating severity_analysis table");

        // First, clear the table if it has data
        hiveUtils.executeUpdate("TRUNCATE TABLE severity_analysis");

        // Create a query that finds common time of day and weather per severity level
        String insertSeveritySQL =
                "INSERT INTO severity_analysis " +
                        "WITH severity_time_weather AS ( " +
                        "  SELECT " +
                        "    severity as severity_level, " +
                        "    CASE " +
                        "      WHEN HOUR(start_time) BETWEEN 6 AND 11 THEN 'Morning' " +
                        "      WHEN HOUR(start_time) BETWEEN 12 AND 17 THEN 'Afternoon' " +
                        "      WHEN HOUR(start_time) BETWEEN 18 AND 23 THEN 'Evening' " +
                        "      ELSE 'Night' " +
                        "    END as time_of_day, " +
                        "    weather_condition, " +
                        "    COUNT(*) as weather_count " +
                        "  FROM " + rawTableName + " " +
                        "  WHERE weather_condition IS NOT NULL " +
                        "  GROUP BY severity, " +
                        "    CASE " +
                        "      WHEN HOUR(start_time) BETWEEN 6 AND 11 THEN 'Morning' " +
                        "      WHEN HOUR(start_time) BETWEEN 12 AND 17 THEN 'Afternoon' " +
                        "      WHEN HOUR(start_time) BETWEEN 18 AND 23 THEN 'Evening' " +
                        "      ELSE 'Night' " +
                        "    END, " +
                        "    weather_condition " +
                        "), " +
                        "ranked_time_weather AS ( " +
                        "  SELECT " +
                        "    severity_level, " +
                        "    time_of_day, " +
                        "    weather_condition, " +
                        "    weather_count, " +
                        "    ROW_NUMBER() OVER (PARTITION BY severity_level ORDER BY weather_count DESC) as time_rank, " +
                        "    ROW_NUMBER() OVER (PARTITION BY severity_level ORDER BY weather_count DESC) as weather_rank " +
                        "  FROM severity_time_weather " +
                        ") " +
                        "SELECT " +
                        "  s.severity_level, " +
                        "  CASE " +
                        "    WHEN s.severity_level = 1 THEN 'Minor' " +
                        "    WHEN s.severity_level = 2 THEN 'Moderate' " +
                        "    WHEN s.severity_level = 3 THEN 'Serious' " +
                        "    WHEN s.severity_level = 4 THEN 'Severe' " +
                        "    ELSE 'Unknown' " +
                        "  END as severity_description, " +
                        "  t.time_of_day as common_time_of_day, " +
                        "  w.weather_condition as common_weather_condition, " +
                        "  COUNT(*) as count_by_severity " +
                        "FROM " + rawTableName + " s " +
                        "LEFT JOIN (SELECT severity_level, time_of_day FROM ranked_time_weather WHERE time_rank = 1) t " +
                        "  ON s.severity = t.severity_level " +
                        "LEFT JOIN (SELECT severity_level, weather_condition FROM ranked_time_weather WHERE weather_rank = 1) w " +
                        "  ON s.severity = w.severity_level " +
                        "GROUP BY " +
                        "  s.severity, " +
                        "  CASE " +
                        "    WHEN s.severity = 1 THEN 'Minor' " +
                        "    WHEN s.severity = 2 THEN 'Moderate' " +
                        "    WHEN s.severity = 3 THEN 'Serious' " +
                        "    WHEN s.severity = 4 THEN 'Severe' " +
                        "    ELSE 'Unknown' " +
                        "  END, " +
                        "  t.time_of_day, " +
                        "  w.weather_condition";

        hiveUtils.executeUpdate(insertSeveritySQL);
        logger.info("Severity analysis table populated");
    }

    /**
     * Populate the time_analysis table
     */
    private void populateTimeAnalysisTable() {
        logger.info("Populating time_analysis table");

        // First, clear the table if it has data
        hiveUtils.executeUpdate("TRUNCATE TABLE time_analysis");

        String insertTimeSQL =
                "INSERT INTO time_analysis " +
                        "SELECT " +
                        "  HOUR(start_time) as hour_of_day, " +
                        "  CASE " +
                        "    WHEN DAYOFWEEK(start_time) = 1 THEN 'Sunday' " +
                        "    WHEN DAYOFWEEK(start_time) = 2 THEN 'Monday' " +
                        "    WHEN DAYOFWEEK(start_time) = 3 THEN 'Tuesday' " +
                        "    WHEN DAYOFWEEK(start_time) = 4 THEN 'Wednesday' " +
                        "    WHEN DAYOFWEEK(start_time) = 5 THEN 'Thursday' " +
                        "    WHEN DAYOFWEEK(start_time) = 6 THEN 'Friday' " +
                        "    WHEN DAYOFWEEK(start_time) = 7 THEN 'Saturday' " +
                        "  END as day_of_week, " +
                        "  CASE " +
                        "    WHEN MONTH(start_time) = 1 THEN 'January' " +
                        "    WHEN MONTH(start_time) = 2 THEN 'February' " +
                        "    WHEN MONTH(start_time) = 3 THEN 'March' " +
                        "    WHEN MONTH(start_time) = 4 THEN 'April' " +
                        "    WHEN MONTH(start_time) = 5 THEN 'May' " +
                        "    WHEN MONTH(start_time) = 6 THEN 'June' " +
                        "    WHEN MONTH(start_time) = 7 THEN 'July' " +
                        "    WHEN MONTH(start_time) = 8 THEN 'August' " +
                        "    WHEN MONTH(start_time) = 9 THEN 'September' " +
                        "    WHEN MONTH(start_time) = 10 THEN 'October' " +
                        "    WHEN MONTH(start_time) = 11 THEN 'November' " +
                        "    WHEN MONTH(start_time) = 12 THEN 'December' " +
                        "  END as month_of_year, " +
                        "  YEAR(start_time) as year, " +
                        "  sunrise_sunset as sunrise_sunset_period, " +
                        "  COALESCE(civil_twilight, nautical_twilight, astronomical_twilight, 'Unknown') as twilight_period, " +
                        "  COUNT(*) as accident_count " +
                        "FROM " + rawTableName + " " +
                        "WHERE start_time IS NOT NULL " +
                        "GROUP BY " +
                        "  HOUR(start_time), " +
                        "  CASE " +
                        "    WHEN DAYOFWEEK(start_time) = 1 THEN 'Sunday' " +
                        "    WHEN DAYOFWEEK(start_time) = 2 THEN 'Monday' " +
                        "    WHEN DAYOFWEEK(start_time) = 3 THEN 'Tuesday' " +
                        "    WHEN DAYOFWEEK(start_time) = 4 THEN 'Wednesday' " +
                        "    WHEN DAYOFWEEK(start_time) = 5 THEN 'Thursday' " +
                        "    WHEN DAYOFWEEK(start_time) = 6 THEN 'Friday' " +
                        "    WHEN DAYOFWEEK(start_time) = 7 THEN 'Saturday' " +
                        "  END, " +
                        "  CASE " +
                        "    WHEN MONTH(start_time) = 1 THEN 'January' " +
                        "    WHEN MONTH(start_time) = 2 THEN 'February' " +
                        "    WHEN MONTH(start_time) = 3 THEN 'March' " +
                        "    WHEN MONTH(start_time) = 4 THEN 'April' " +
                        "    WHEN MONTH(start_time) = 5 THEN 'May' " +
                        "    WHEN MONTH(start_time) = 6 THEN 'June' " +
                        "    WHEN MONTH(start_time) = 7 THEN 'July' " +
                        "    WHEN MONTH(start_time) = 8 THEN 'August' " +
                        "    WHEN MONTH(start_time) = 9 THEN 'September' " +
                        "    WHEN MONTH(start_time) = 10 THEN 'October' " +
                        "    WHEN MONTH(start_time) = 11 THEN 'November' " +
                        "    WHEN MONTH(start_time) = 12 THEN 'December' " +
                        "  END, " +
                        "  YEAR(start_time), " +
                        "  sunrise_sunset, " +
                        "  COALESCE(civil_twilight, nautical_twilight, astronomical_twilight, 'Unknown')";

        hiveUtils.executeUpdate(insertTimeSQL);
        logger.info("Time analysis table populated");
    }

    /**
     * Populate the weather_analysis table
     */
    private void populateWeatherAnalysisTable() {
        logger.info("Populating weather_analysis table");

        // First, clear the table if it has data
        hiveUtils.executeUpdate("TRUNCATE TABLE weather_analysis");

        String insertWeatherSQL =
                "INSERT INTO weather_analysis " +
                        "SELECT " +
                        "  weather_condition, " +
                        "  CASE " +
                        "    WHEN temperature < 32 THEN 'Below Freezing (< 32°F)' " +
                        "    WHEN temperature BETWEEN 32 AND 50 THEN 'Cold (32-50°F)' " +
                        "    WHEN temperature BETWEEN 50 AND 68 THEN 'Mild (50-68°F)' " +
                        "    WHEN temperature BETWEEN 68 AND 86 THEN 'Warm (68-86°F)' " +
                        "    WHEN temperature > 86 THEN 'Hot (> 86°F)' " +
                        "    ELSE 'Unknown' " +
                        "  END as temperature_range, " +
                        "  CASE " +
                        "    WHEN visibility < 1 THEN 'Very Low (< 1 mile)' " +
                        "    WHEN visibility BETWEEN 1 AND 3 THEN 'Low (1-3 miles)' " +
                        "    WHEN visibility BETWEEN 3 AND 7 THEN 'Moderate (3-7 miles)' " +
                        "    WHEN visibility > 7 THEN 'Good (> 7 miles)' " +
                        "    ELSE 'Unknown' " +
                        "  END as visibility_range, " +
                        "  CASE " +
                        "    WHEN precipitation = 0 THEN 'None' " +
                        "    WHEN precipitation BETWEEN 0 AND 0.1 THEN 'Light (0-0.1 in)' " +
                        "    WHEN precipitation BETWEEN 0.1 AND 0.5 THEN 'Moderate (0.1-0.5 in)' " +
                        "    WHEN precipitation > 0.5 THEN 'Heavy (> 0.5 in)' " +
                        "    ELSE 'Unknown' " +
                        "  END as precipitation_level, " +
                        "  CASE " +
                        "    WHEN wind_speed < 5 THEN 'Calm (< 5 mph)' " +
                        "    WHEN wind_speed BETWEEN 5 AND 15 THEN 'Light (5-15 mph)' " +
                        "    WHEN wind_speed BETWEEN 15 AND 25 THEN 'Moderate (15-25 mph)' " +
                        "    WHEN wind_speed > 25 THEN 'Strong (> 25 mph)' " +
                        "    ELSE 'Unknown' " +
                        "  END as wind_speed_range, " +
                        "  AVG(severity) as average_severity, " +
                        "  COUNT(*) as accident_count " +
                        "FROM " + rawTableName + " " +
                        "WHERE weather_condition IS NOT NULL " +
                        "GROUP BY " +
                        "  weather_condition, " +
                        "  CASE " +
                        "    WHEN temperature < 32 THEN 'Below Freezing (< 32°F)' " +
                        "    WHEN temperature BETWEEN 32 AND 50 THEN 'Cold (32-50°F)' " +
                        "    WHEN temperature BETWEEN 50 AND 68 THEN 'Mild (50-68°F)' " +
                        "    WHEN temperature BETWEEN 68 AND 86 THEN 'Warm (68-86°F)' " +
                        "    WHEN temperature > 86 THEN 'Hot (> 86°F)' " +
                        "    ELSE 'Unknown' " +
                        "  END, " +
                        "  CASE " +
                        "    WHEN visibility < 1 THEN 'Very Low (< 1 mile)' " +
                        "    WHEN visibility BETWEEN 1 AND 3 THEN 'Low (1-3 miles)' " +
                        "    WHEN visibility BETWEEN 3 AND 7 THEN 'Moderate (3-7 miles)' " +
                        "    WHEN visibility > 7 THEN 'Good (> 7 miles)' " +
                        "    ELSE 'Unknown' " +
                        "  END, " +
                        "  CASE " +
                        "    WHEN precipitation = 0 THEN 'None' " +
                        "    WHEN precipitation BETWEEN 0 AND 0.1 THEN 'Light (0-0.1 in)' " +
                        "    WHEN precipitation BETWEEN 0.1 AND 0.5 THEN 'Moderate (0.1-0.5 in)' " +
                        "    WHEN precipitation > 0.5 THEN 'Heavy (> 0.5 in)' " +
                        "    ELSE 'Unknown' " +
                        "  END, " +
                        "  CASE " +
                        "    WHEN wind_speed < 5 THEN 'Calm (< 5 mph)' " +
                        "    WHEN wind_speed BETWEEN 5 AND 15 THEN 'Light (5-15 mph)' " +
                        "    WHEN wind_speed BETWEEN 15 AND 25 THEN 'Moderate (15-25 mph)' " +
                        "    WHEN wind_speed > 25 THEN 'Strong (> 25 mph)' " +
                        "    ELSE 'Unknown' " +
                        "  END " +
                        "HAVING COUNT(*) > 10";

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
}