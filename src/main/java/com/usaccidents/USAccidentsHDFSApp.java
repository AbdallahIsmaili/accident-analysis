package com.usaccidents;

import com.usaccidents.io.HiveUtils;
import com.usaccidents.hive.USAccidentsHiveDataProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Main application class for US Accidents Analysis using HDFS
 */
public class USAccidentsHDFSApp {
    private static final Logger logger = LoggerFactory.getLogger(USAccidentsHDFSApp.class);
    private static final String HDFS_OUTPUT_DIR = "/user/" + System.getProperty("user.name") + "/us_accidents_output";

    public static void main(String[] args) {
        // Initialize Hive connection
        HiveUtils hiveUtils = new HiveUtils("jdbc:hive2://localhost:10000/us_accidents", "", "");
        hiveUtils.connect();

        try {
            // Create and run the processor
            USAccidentsHiveDataProcessor processor =
                    new USAccidentsHiveDataProcessor(hiveUtils, "raw_accidents");

            // Execute the analysis workflow
//            processor.executeAnalysisOnly();

            // Display sample data from all tables
            logger.info("Displaying sample data from all tables...");
            processor.selectSampleFromAllTables();

            // Test just the HDFS CSV export functionality
            logger.info("Testing HDFS CSV export functionality...");
            processor.saveAllAnalysisTablesToHdfsCSV(HDFS_OUTPUT_DIR);


            logger.info("ENDED: CSV export completed successfully");
        } catch (Exception e) {
            logger.error("Error during processing: {}", e.getMessage(), e);
            System.err.println("Error during processing: " + e.getMessage());
        } finally {
            // Close connection
            hiveUtils.close();
        }
    }
}