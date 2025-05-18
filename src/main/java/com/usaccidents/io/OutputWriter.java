package com.usaccidents.io;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

/**
 * Handles writing analysis results to output files
 */
public class OutputWriter {
    private static final Logger logger = LoggerFactory.getLogger(OutputWriter.class);
    private final String outputDirectory;
    private final DateTimeFormatter fileNameFormatter = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss");
    private final boolean isHdfs;
    private HDFSUtils hdfsUtils;

    /**
     * Constructor for local file system output
     */
    public OutputWriter(String outputDirectory) {
        this.outputDirectory = outputDirectory;
        this.isHdfs = false;
    }

    /**
     * Constructor for HDFS output
     */
    public OutputWriter(String outputDirectory, HDFSUtils hdfsUtils) {
        this.outputDirectory = outputDirectory;
        this.hdfsUtils = hdfsUtils;
        this.isHdfs = true;

        if (isHdfs && hdfsUtils != null) {
            hdfsUtils.createDirectoryIfNotExists(outputDirectory);
        }
    }

    /**
     * Write analysis results to files
     */
    public void writeResults(Map<String, Object> results) {
        try {
            // Create the output directory if it doesn't exist (for local filesystem)
            if (!isHdfs) {
                Path outputPath = Paths.get(outputDirectory);
                if (!Files.exists(outputPath)) {
                    Files.createDirectories(outputPath);
                }
            }

            String timestamp = LocalDateTime.now().format(fileNameFormatter);

            // Write summary report
            writeSummaryReport(results, timestamp);

            // Write detailed reports
            writeDetailedReport(results, "accidents_by_state", (List<Map.Entry<String, Integer>>) results.get("topStatesByAccidentCount"), timestamp);
            writeDetailedReport(results, "accidents_by_severity", results.get("accidentsBySeverity"), timestamp);
            writeDetailedReport(results, "accidents_by_weather", (List<Map.Entry<String, Integer>>) results.get("topWeatherConditions"), timestamp);
            writeDetailedReport(results, "accidents_by_hour", results.get("accidentsByHour"), timestamp);

            logger.info("All results written to {}", outputDirectory);
        } catch (IOException e) {
            logger.error("Error writing results", e);
            throw new RuntimeException("Failed to write analysis results", e);
        }
    }

    /**
     * Write a summary report
     */
    private void writeSummaryReport(Map<String, Object> results, String timestamp) {
        String fileName = "summary_report_" + timestamp + ".txt";
        StringBuilder content = new StringBuilder("US Accidents Analysis Summary Report\n");
        content.append("Generated: ").append(LocalDateTime.now()).append("\n\n");
        content.append("Total accidents analyzed: ").append(results.get("totalAccidents")).append("\n");

        if (results.containsKey("averageSeverity")) {
            content.append("Average accident severity: ").append(String.format("%.2f", results.get("averageSeverity"))).append("\n");
        }

        content.append("\nTop States by Accident Count:\n");
        List<Map.Entry<String, Integer>> topStates = (List<Map.Entry<String, Integer>>) results.get("topStatesByAccidentCount");
        if (topStates != null) {
            for (Map.Entry<String, Integer> entry : topStates) {
                content.append("  - ").append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
            }
        }

        content.append("\nAccidents by Severity:\n");
        Map<Integer, Integer> severityMap = (Map<Integer, Integer>) results.get("accidentsBySeverity");
        if (severityMap != null) {
            for (Map.Entry<Integer, Integer> entry : severityMap.entrySet()) {
                content.append("  - Severity ").append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
            }
        }

        writeToFile(fileName, content.toString());
    }

    /**
     * Write a detailed report for a specific category
     */
    private void writeDetailedReport(Map<String, Object> results, String reportName, Object data, String timestamp) {
        String fileName = reportName + "_" + timestamp + ".txt";
        StringBuilder content = new StringBuilder("US Accidents Detailed Report: " + reportName + "\n");
        content.append("Generated: ").append(LocalDateTime.now()).append("\n\n");
        content.append("Total accidents analyzed: ").append(results.get("totalAccidents")).append("\n\n");

        if (data instanceof List) {
            List<Map.Entry<String, Integer>> entries = (List<Map.Entry<String, Integer>>) data;
            for (Map.Entry<String, Integer> entry : entries) {
                content.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
            }
        } else if (data instanceof Map) {
            Map<?, Integer> map = (Map<?, Integer>) data;
            for (Map.Entry<?, Integer> entry : map.entrySet()) {
                content.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
            }
        }

        writeToFile(fileName, content.toString());
    }

    /**
     * Write content to a file (local or HDFS)
     */
    private void writeToFile(String fileName, String content) {
        String filePath = outputDirectory + "/" + fileName;

        try {
            if (isHdfs && hdfsUtils != null) {
                // Write to HDFS
                hdfsUtils.writeToFile(filePath, content);
                logger.info("Wrote results to HDFS file: {}", filePath);
            } else {
                // Write to local file system
                try (FileWriter writer = new FileWriter(filePath)) {
                    writer.write(content);
                }
                logger.info("Wrote results to local file: {}", filePath);
            }
        } catch (IOException e) {
            logger.error("Error writing to file: {}", filePath, e);
            throw new RuntimeException("Failed to write to file", e);
        }
    }
}