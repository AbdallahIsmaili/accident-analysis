package com.usaccidents;

import com.usaccidents.operators.AccidentAnalyzer;
import com.usaccidents.io.CSVParser;
import com.usaccidents.io.OutputWriter;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Main application class for US Accidents Analysis
 * Standalone version without Apache Apex dependencies
 */
public class USAccidentsStandaloneApp {

    public static void main(String[] args) {
        try {
            // Default input and output directories
            String inputDir = "/user/hadoop/us-accidents";
            String outputDir = "/tmp/usaccidents-output";

            // Override defaults if provided as command line arguments
            if (args.length >= 1) {
                inputDir = args[0];
            }
            if (args.length >= 2) {
                outputDir = args[1];
            }

            System.out.println("Starting US Accidents Analysis");
            System.out.println("Input directory: " + inputDir);
            System.out.println("Output directory: " + outputDir);

            // Create output directory if it doesn't exist
            Path outputPath = Paths.get(outputDir);
            if (!Files.exists(outputPath)) {
                Files.createDirectories(outputPath);
            }

            // Find all CSV files in the input directory
            List<File> csvFiles = findCsvFiles(inputDir);
            System.out.println("Found " + csvFiles.size() + " CSV files to process");

            if (csvFiles.isEmpty()) {
                System.out.println("No CSV files found in " + inputDir);
                return;
            }

            // Create analyzer and output writer
            AccidentAnalyzer analyzer = new AccidentAnalyzer();
            OutputWriter writer = new OutputWriter(outputDir);

            // Process each CSV file
            for (File csvFile : csvFiles) {
                System.out.println("Processing file: " + csvFile.getAbsolutePath());

                // Parse CSV file and process each accident
                CSVParser parser = new CSVParser();
                parser.parseCSVFile(csvFile, accident -> {
                    analyzer.processAccident(accident);
                });

                // Get analysis results and write them
                Map<String, Object> results = analyzer.getResults();
                writer.writeResults(results);

                System.out.println("Completed processing file: " + csvFile.getName());
                System.out.println("Total accidents processed: " + analyzer.getTotalAccidents());
            }

            System.out.println("Analysis complete. Results written to " + outputDir);

        } catch (Exception e) {
            System.err.println("Error processing accidents data: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static List<File> findCsvFiles(String directoryPath) {
        File directory = new File(directoryPath);
        File[] files = directory.listFiles((dir, name) -> name.toLowerCase().endsWith(".csv"));

        if (files == null) {
            return Collections.emptyList(); // Java 8 compatible alternative to List.of()
        }

        return Arrays.asList(files); // Java 8 compatible alternative to List.of(files)
    }
}