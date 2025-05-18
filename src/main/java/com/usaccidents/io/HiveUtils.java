package com.usaccidents.io;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility class for Hive operations
 */
public class HiveUtils {
    private static final Logger logger = LoggerFactory.getLogger(HiveUtils.class);
    private String jdbcURL;
    private String username;
    private String password;
    private Connection connection;

    public HiveUtils() {
        // Default connection to local Hive instance
        this("jdbc:hive2://localhost:10000/default", "", "");
    }

    public HiveUtils(String jdbcURL, String username, String password) {
        this.jdbcURL = jdbcURL;
        this.username = username;
        this.password = password;
    }

    /**
     * Connect to Hive
     */
    public void connect() {
        try {
            // Make sure the Hive JDBC driver is loaded
            Class.forName("org.apache.hive.jdbc.HiveDriver");

            logger.info("Connecting to Hive with URL: {}", jdbcURL);
            connection = DriverManager.getConnection(jdbcURL, username, password);
            logger.info("Successfully connected to Hive");
        } catch (ClassNotFoundException e) {
            logger.error("Hive JDBC driver not found", e);
            throw new RuntimeException("Hive JDBC driver not found", e);
        } catch (SQLException e) {
            logger.error("Failed to connect to Hive", e);
            throw new RuntimeException("Failed to connect to Hive", e);
        }
    }

    /**
     * Create a table in Hive for US accidents data (if not exists)
     */
    public void createAccidentsTable(String tableName) {
        String createTableSQL = "CREATE TABLE IF NOT EXISTS " + tableName + " (" +
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
                "STORED AS TEXTFILE";

        executeUpdate(createTableSQL);
        logger.info("Created or verified table: {}", tableName);
    }

    /**
     * Load data from HDFS into Hive table
     */
    public void loadDataFromHDFS(String tableName, String hdfsFilePath) {
        String loadDataSQL = "LOAD DATA INPATH '" + hdfsFilePath + "' OVERWRITE INTO TABLE " + tableName;
        executeUpdate(loadDataSQL);
        logger.info("Loaded data from {} into table {}", hdfsFilePath, tableName);
    }

    /**
     * Execute a Hive query and return results
     */
    public List<Map<String, Object>> executeQuery(String query) {
        List<Map<String, Object>> results = new ArrayList<>();

        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {

            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (resultSet.next()) {
                Map<String, Object> row = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);
                    Object value = resultSet.getObject(i);
                    row.put(columnName, value);
                }
                results.add(row);
            }

            logger.info("Query executed successfully: {}", query);
        } catch (SQLException e) {
            logger.error("Error executing query: " + query, e);
            throw new RuntimeException("Error executing Hive query", e);
        }

        return results;
    }

    /**
     * Execute a Hive update statement (DDL or DML)
     */
    public void executeUpdate(String sql) {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
            logger.info("SQL statement executed successfully: {}", sql);
        } catch (SQLException e) {
            logger.error("Error executing SQL: " + sql, e);
            throw new RuntimeException("Error executing Hive SQL", e);
        }
    }

    /**
     * Close the Hive connection
     */
    public void close() {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
                logger.info("Closed Hive connection");
            }
        } catch (SQLException e) {
            logger.error("Error closing Hive connection", e);
        }
    }
}