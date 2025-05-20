package com.usaccidents;

import com.usaccidents.io.HDFSUtils;
import com.usaccidents.io.HiveUtils;
import com.usaccidents.io.CSVParser;
import com.usaccidents.io.OutputWriter;
import com.usaccidents.operators.AccidentAnalyzer;
import com.usaccidents.hive.USAccidentsHiveDataProcessor;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Main application class for US Accidents Analysis using HDFS
 */
public class USAccidentsHDFSApp {
    public static void main(String[] args) {
        // Initialize Hive connection
        HiveUtils hiveUtils = new HiveUtils("jdbc:hive2://localhost:10000/us_accidents", "", "");
        hiveUtils.connect();

        // Create and run the processor
        USAccidentsHiveDataProcessor processor =
                new USAccidentsHiveDataProcessor(hiveUtils, "raw_accidents");
        processor.executeAnalysisOnly();

        // Close connection
        hiveUtils.close();
    }
}