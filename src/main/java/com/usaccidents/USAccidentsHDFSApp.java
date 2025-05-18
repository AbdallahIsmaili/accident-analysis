package com.usaccidents;

import com.usaccidents.io.HDFSUtils;
import com.usaccidents.io.HiveUtils;
import com.usaccidents.io.CSVParser;
import com.usaccidents.io.OutputWriter;
import com.usaccidents.operators.AccidentAnalyzer;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Main application class for US Accidents Analysis using HDFS
 */
public class USAccidentsHDFSApp {
    private static final Logger logger = LoggerFactory.getLogger(USAccidentsHDFSApp.class);

    public static void main(String[] args) {
        try {
            // Default HDFS input and output directories
            String hdfsInputDir = "/user/hadoop/us-accidents";
            String hdfsOutputDir = "/tmp/usaccidents-output";
            boolean useHive = false;
            String hiveJdbcUrl = "jdbc:hive2://localhost:10000/default";

            // Parse command line arguments
            for (int i = 0; i < args.length; i++) {
                if (args[i].equals("--input") && i + 1 < args.length) {
                    hdfsInputDir = args[i + 1];
                    i++;
                } else if (args[i].equals("--output") && i + 1 < args.length) {
                    hdfsOutputDir = args[i + 1];
                    i++;
                } else if (args[i].equals("--use-hive")) {
                    useHive = true;
                } else if (args[i].equals("--hive-jdbc") && i + 1 < args.length) {
                    hiveJdbcUrl = args[i + 1];
                    i++;
                }
            }

            logger.info("Starting US Accidents Analysis with HDFS");
            logger.info("HDFS Input directory: {}", hdfsInputDir);
            logger.info("HDFS Output directory: {}", hdfsOutputDir);
            logger.info("Use Hive: {}", useHive);

            // Initialize HDFS utilities
            HDFSUtils hdfsUtils = new HDFSUtils();

            // Create output directory in HDFS
            hdfsUtils.createDirectoryIfNotExists(hdfsOutputDir);

            // Initialize Hive if requested
            HiveUtils hiveUtils = null;
            if (useHive) {
                logger.info("Initializing Hive connection with URL: {}", hiveJdbcUrl);
                hiveUtils = new HiveUtils(hiveJdbcUrl, "", "");
                hiveUtils.connect();

                // Create accidents table in Hive
                hiveUtils.createAccidentsTable("us_accidents");
            }

            // Find all CSV files in the HDFS input directory
            List<Path> csvFiles = hdfsUtils.findCsvFiles(hdfsInputDir);
            logger.info("Found {} CSV files to process", csvFiles.size());

            if (csvFiles.isEmpty()) {
                logger.warn("No CSV files found in {}", hdfsInputDir);
                return;
            }

            // Create analyzer and output writer
            AccidentAnalyzer analyzer = new AccidentAnalyzer();
            OutputWriter writer = new OutputWriter(hdfsOutputDir, hdfsUtils);

            // Process each CSV file
            for (Path csvFile : csvFiles) {
                logger.info("Processing HDFS file: {}", csvFile.toString());

                // If using Hive, load data into Hive table
                if (useHive && hiveUtils != null) {
                    hiveUtils.loadDataFromHDFS("us_accidents", csvFile.toString());
                    logger.info("Loaded data from {} into Hive table us_accidents", csvFile.toString());

                    // For demonstration, let's run a simple query
                    List<Map<String, Object>> results = hiveUtils.executeQuery(
                            "SELECT state, COUNT(*) as count FROM us_accidents GROUP BY state ORDER BY count DESC LIMIT 10");

                    logger.info("Top 10 states by accident count from Hive:");
                    for (Map<String, Object> row : results) {
                        logger.info("  {} - {}", row.get("state"), row.get("count"));
                    }
                }

                // Parse CSV file and process each accident
                CSVParser parser = new CSVParser();
                parser.parseHDFSCSVFile(csvFile, hdfsUtils, accident -> {
                    analyzer.processAccident(accident);
                });

                logger.info("Completed processing file: {}", csvFile.getName());
                logger.info("Total accidents processed so far: {}", analyzer.getTotalAccidents());
            }

            // Get analysis results and write them
            Map<String, Object> results = analyzer.getResults();
            writer.writeResults(results);

            logger.info("Analysis complete. Results written to {}", hdfsOutputDir);

            // Close resources
            if (hiveUtils != null) {
                hiveUtils.close();
            }
            hdfsUtils.close();

        } catch (Exception e) {
            logger.error("Error processing accidents data: {}", e.getMessage(), e);
            System.exit(1);
        }
    }
}