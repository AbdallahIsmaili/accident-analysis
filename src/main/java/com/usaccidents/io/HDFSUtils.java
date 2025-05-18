package com.usaccidents.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Utility class for HDFS operations
 */
public class HDFSUtils {
    private static final Logger logger = LoggerFactory.getLogger(HDFSUtils.class);
    private Configuration configuration;
    private FileSystem fileSystem;

    public HDFSUtils() {
        try {
            configuration = new Configuration();
            // Important: Configure the fs.defaultFS property if needed
            // configuration.set("fs.defaultFS", "hdfs://localhost:9000");
            fileSystem = FileSystem.get(configuration);
            logger.info("Initialized HDFS file system: {}", fileSystem.getUri());
        } catch (IOException e) {
            logger.error("Failed to initialize HDFS file system", e);
            throw new RuntimeException("Failed to initialize HDFS file system", e);
        }
    }

    /**
     * Connect to a specific HDFS using URI
     */
    public HDFSUtils(String hdfsUri) {
        try {
            configuration = new Configuration();
            fileSystem = FileSystem.get(URI.create(hdfsUri), configuration);
            logger.info("Initialized HDFS file system with URI: {}", hdfsUri);
        } catch (IOException e) {
            logger.error("Failed to initialize HDFS file system with URI: " + hdfsUri, e);
            throw new RuntimeException("Failed to initialize HDFS file system", e);
        }
    }

    /**
     * Find all CSV files in the given HDFS directory
     */
    public List<Path> findCsvFiles(String directoryPath) {
        List<Path> csvFiles = new ArrayList<>();
        try {
            Path hdfsPath = new Path(directoryPath);
            if (!fileSystem.exists(hdfsPath)) {
                logger.error("Directory does not exist: {}", directoryPath);
                return csvFiles;
            }

            FileStatus[] fileStatuses = fileSystem.listStatus(hdfsPath);
            for (FileStatus status : fileStatuses) {
                if (!status.isDirectory() && status.getPath().getName().toLowerCase().endsWith(".csv")) {
                    csvFiles.add(status.getPath());
                    logger.debug("Found CSV file: {}", status.getPath());
                }
            }
            logger.info("Found {} CSV files in {}", csvFiles.size(), directoryPath);
        } catch (IOException e) {
            logger.error("Error listing CSV files in " + directoryPath, e);
        }
        return csvFiles;
    }

    /**
     * Read a CSV file from HDFS and process each line
     */
    public void readCSVFile(Path filePath, Consumer<String> lineProcessor) {
        try (FSDataInputStream inputStream = fileSystem.open(filePath);
             BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {

            String line;
            boolean isHeader = true;
            while ((line = reader.readLine()) != null) {
                if (isHeader) {
                    isHeader = false;
                    continue; // Skip header line
                }
                lineProcessor.accept(line);
            }
            logger.info("Finished reading file: {}", filePath);
        } catch (IOException e) {
            logger.error("Error reading file: " + filePath, e);
            throw new RuntimeException("Error reading HDFS file", e);
        }
    }

    /**
     * Write content to an HDFS file
     */
    public void writeToFile(String filePath, String content) {
        try (org.apache.hadoop.fs.FSDataOutputStream outputStream = fileSystem.create(new Path(filePath), true)) {
            outputStream.writeBytes(content);
            logger.info("Successfully wrote to file: {}", filePath);
        } catch (IOException e) {
            logger.error("Error writing to file: " + filePath, e);
            throw new RuntimeException("Error writing to HDFS file", e);
        }
    }

    /**
     * Create directory in HDFS if it doesn't exist
     */
    public void createDirectoryIfNotExists(String dirPath) {
        try {
            Path path = new Path(dirPath);
            if (!fileSystem.exists(path)) {
                fileSystem.mkdirs(path);
                logger.info("Created directory in HDFS: {}", dirPath);
            }
        } catch (IOException e) {
            logger.error("Error creating directory: " + dirPath, e);
            throw new RuntimeException("Error creating HDFS directory", e);
        }
    }

    /**
     * Close the HDFS file system connection
     */
    public void close() {
        try {
            if (fileSystem != null) {
                fileSystem.close();
                logger.info("Closed HDFS file system connection");
            }
        } catch (IOException e) {
            logger.error("Error closing HDFS file system", e);
        }
    }
}