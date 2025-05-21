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
    private static final String OUTPUT_DIR = "D:\\Global Projects\\Java\\datatorrent\\accident-analysis\\us-accidents-output-csv";

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
//
//            // Display sample data from all tables
//            logger.info("Displaying sample data from all tables...");
//            processor.selectSampleFromAllTables();

            // Save analysis tables to CSV
            logger.info("Exporting analysis tables to CSV files...");
            processor.saveAllAnalysisTablesToCSV(OUTPUT_DIR);

            logger.info("ENDED: CSV export test completed successfully");
        } catch (Exception e) {
            logger.error("Error during processing: {}", e.getMessage(), e);
            System.err.println("Error during processing: " + e.getMessage());
        } finally {
            // Close connection
            hiveUtils.close();
        }
    }
}