package com.usaccidents.io;

import com.usaccidents.model.Accident;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.function.Consumer;

/**
 * Parser for CSV files with US Accidents data
 */
public class CSVParser {
    private static final Logger logger = LoggerFactory.getLogger(CSVParser.class);
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * Parse a local CSV file and process each accident record
     */
    public void parseCSVFile(File file, Consumer<Accident> processor) {
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line = reader.readLine(); // Skip header line

            if (line == null) {
                logger.warn("Empty CSV file: {}", file.getName());
                return;
            }

            // Process each line
            while ((line = reader.readLine()) != null) {
                try {
                    Accident accident = parseAccidentLine(line);
                    if (accident != null) {
                        processor.accept(accident);
                    }
                } catch (Exception e) {
                    logger.error("Error processing line: {}", line, e);
                }
            }
        } catch (IOException e) {
            logger.error("Error reading CSV file: {}", file.getName(), e);
            throw new RuntimeException("Error reading CSV file", e);
        }
    }

    /**
     * Parse an HDFS CSV file and process each accident record
     */
    public void parseHDFSCSVFile(Path hdfsPath, HDFSUtils hdfsUtils, Consumer<Accident> processor) {
        hdfsUtils.readCSVFile(hdfsPath, line -> {
            try {
                Accident accident = parseAccidentLine(line);
                if (accident != null) {
                    processor.accept(accident);
                }
            } catch (Exception e) {
                logger.error("Error processing line from HDFS: {}", line, e);
            }
        });
    }

    /**
     * Parse a CSV line into an Accident object
     */
    private Accident parseAccidentLine(String line) {
        if (line == null || line.trim().isEmpty()) {
            return null;
        }

        // Split by comma, but avoid splitting commas inside quotes
        String[] fields = splitCSVLine(line);

        if (fields.length < 10) {
            logger.warn("Insufficient fields in line: {}", line);
            return null;
        }

        try {
            Accident accident = new Accident();

            // Set basic fields from CSV
            accident.setId(getStringValue(fields, 0));
            accident.setSeverity(getIntValue(fields, 1));
            accident.setStartTime(getDateTimeValue(fields, 2));
            accident.setEndTime(getDateTimeValue(fields, 3));
            accident.setStartLat(getDoubleValue(fields, 4));
            accident.setStartLng(getDoubleValue(fields, 5));

            // Set location fields
            accident.setStreet(getStringValue(fields, 10));
            accident.setCity(getStringValue(fields, 11));
            accident.setState(getStringValue(fields, 13));

            // Set weather fields if available
            if (fields.length > 25) {
                accident.setWeatherCondition(getStringValue(fields, 25));
            }
            if (fields.length > 17) {
                accident.setTemperature(getDoubleValue(fields, 17));
            }

            return accident;
        } catch (Exception e) {
            logger.error("Failed to parse accident data from line: {}", line, e);
            return null;
        }
    }

    /**
     * Split CSV line, handling quoted fields
     */
    private String[] splitCSVLine(String line) {
        // Basic CSV parsing logic - this doesn't handle all CSV edge cases
        // For production, consider using a library like Apache Commons CSV
        return line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
    }

    private String getStringValue(String[] fields, int index) {
        if (index >= fields.length) return "";
        String value = fields[index].trim();

        // Remove surrounding quotes if present
        if (value.startsWith("\"") && value.endsWith("\"")) {
            value = value.substring(1, value.length() - 1);
        }

        return value.isEmpty() ? "" : value;
    }

    private int getIntValue(String[] fields, int index) {
        String value = getStringValue(fields, index);
        try {
            return value.isEmpty() ? 0 : Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private double getDoubleValue(String[] fields, int index) {
        String value = getStringValue(fields, index);
        try {
            return value.isEmpty() ? 0.0 : Double.parseDouble(value);
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }

    private LocalDateTime getDateTimeValue(String[] fields, int index) {
        String value = getStringValue(fields, index);
        try {
            return value.isEmpty() ? null : LocalDateTime.parse(value, DATE_FORMATTER);
        } catch (DateTimeParseException e) {
            return null;
        }
    }
}