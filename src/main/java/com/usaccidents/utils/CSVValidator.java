package com.usaccidents.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * Utility class to test and validate US Accidents CSV files
 * This can be used to check if the CSV format is compatible with the application
 */
public class CSVValidator {

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: java com.usaccidents.utils.CSVValidator <path-to-csv-file>");
            return;
        }

        String csvFilePath = args[0];
        System.out.println("Validating CSV file: " + csvFilePath);

        try {
            validateCSVFile(csvFilePath);
            System.out.println("CSV validation completed successfully.");
        } catch (Exception e) {
            System.err.println("Error validating CSV file: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void validateCSVFile(String filePath) throws IOException {
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(filePath));

            // Read and validate headers
            String headerLine = reader.readLine();
            if (headerLine == null) {
                throw new IOException("CSV file is empty");
            }

            String[] headers = headerLine.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
            System.out.println("Found " + headers.length + " columns in the CSV header");

            // Display header names
            System.out.println("\nHeaders detected:");
            for (int i = 0; i < headers.length; i++) {
                System.out.println((i+1) + ". " + headers[i].replace("\"", "").trim());
            }

            // Read a sample of data rows
            System.out.println("\nReading sample data rows...");
            int sampleSize = 5;
            int rowCount = 0;
            String line;
            while ((line = reader.readLine()) != null && rowCount < sampleSize) {
                rowCount++;
                String[] fields = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);

                System.out.println("\nRow " + rowCount + " has " + fields.length + " fields");

                // Show a few key fields as a sample
                if (fields.length > 0) {
                    // Assuming these indices exist - adjust based on your actual CSV structure
                    int idIndex = findHeaderIndex(headers, "ID");
                    int severityIndex = findHeaderIndex(headers, "Severity");
                    int stateIndex = findHeaderIndex(headers, "State");

                    System.out.println("Sample data:");
                    if (idIndex >= 0 && idIndex < fields.length) {
                        System.out.println("  ID: " + fields[idIndex].replace("\"", "").trim());
                    }
                    if (severityIndex >= 0 && severityIndex < fields.length) {
                        System.out.println("  Severity: " + fields[severityIndex].replace("\"", "").trim());
                    }
                    if (stateIndex >= 0 && stateIndex < fields.length) {
                        System.out.println("  State: " + fields[stateIndex].replace("\"", "").trim());
                    }
                }
            }

            System.out.println("\nTotal rows sampled: " + rowCount);

            // Continue counting total rows
            long totalRows = rowCount;
            while (reader.readLine() != null) {
                totalRows++;
                if (totalRows % 100000 == 0) {
                    System.out.println("Processed " + totalRows + " rows...");
                }
            }

            System.out.println("\nTotal rows in CSV: " + totalRows);

        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    // Ignore
                }
            }
        }
    }

    private static int findHeaderIndex(String[] headers, String headerName) {
        for (int i = 0; i < headers.length; i++) {
            if (headers[i].replace("\"", "").trim().equalsIgnoreCase(headerName)) {
                return i;
            }
        }
        return -1;
    }
}