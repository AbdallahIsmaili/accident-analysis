package com.usaccidents.operators;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Operator that outputs analysis results to files or other destinations
 */
public class AccidentOutputOperator extends BaseOperator
{
    private static final Logger LOG = LoggerFactory.getLogger(AccidentOutputOperator.class);

    private String outputDirectory = "/tmp/usaccidents-output";
    private OutputStreamWriter writer = null;
    private long fileRotateInterval = 3600000; // Default: 1 hour
    private long lastFileRotateTime = 0;

    @InputPortFieldAnnotation(optional = false)
    public final transient DefaultInputPort<Map<String, Object>> input = new DefaultInputPort<Map<String, Object>>() {
        @Override
        public void process(Map<String, Object> results) {
            checkFileRotation();
            writeResults(results);
        }
    };

    @Override
    public void setup(Context.OperatorContext context) {
        try {
            // Create output directory if it doesn't exist
            File directory = new File(outputDirectory);
            if (!directory.exists()) {
                if (!directory.mkdirs()) {
                    LOG.error("Failed to create output directory: {}", outputDirectory);
                    throw new RuntimeException("Failed to create output directory: " + outputDirectory);
                }
            }

            // Initialize writer
            lastFileRotateTime = System.currentTimeMillis();
            createNewOutputFile();
        } catch (IOException e) {
            LOG.error("Error in operator setup", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void teardown() {
        closeWriter();
    }

    private void checkFileRotation() {
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastFileRotateTime >= fileRotateInterval) {
            closeWriter();
            try {
                createNewOutputFile();
                lastFileRotateTime = currentTime;
            } catch (IOException e) {
                LOG.error("Error rotating output file", e);
            }
        }
    }

    private void createNewOutputFile() throws IOException {
        String timestamp = String.valueOf(System.currentTimeMillis());
        File outputFile = new File(outputDirectory, "accidents-analysis-" + timestamp + ".json");
        writer = new OutputStreamWriter(new FileOutputStream(outputFile), StandardCharsets.UTF_8);
        LOG.info("Created new output file: {}", outputFile.getAbsolutePath());
    }

    private void closeWriter() {
        if (writer != null) {
            try {
                writer.flush();
                writer.close();
                writer = null;
            } catch (IOException e) {
                LOG.error("Error closing writer", e);
            }
        }
    }

    private void writeResults(Map<String, Object> results) {
        if (writer == null) {
            LOG.warn("Writer is null, cannot write results");
            return;
        }

        try {
            // Simple JSON serialization for the results
            StringBuilder json = new StringBuilder("{\n");
            int index = 0;
            for (Map.Entry<String, Object> entry : results.entrySet()) {
                if (index > 0) {
                    json.append(",\n");
                }

                json.append("  \"").append(entry.getKey()).append("\": ");
                appendJsonValue(json, entry.getValue());
                index++;
            }
            json.append("\n}\n");

            writer.write(json.toString());
            writer.flush();
        } catch (IOException e) {
            LOG.error("Error writing results", e);
        }
    }

    @SuppressWarnings("unchecked")
    private void appendJsonValue(StringBuilder json, Object value) {
        if (value == null) {
            json.append("null");
        } else if (value instanceof Number || value instanceof Boolean) {
            json.append(value.toString());
        } else if (value instanceof String) {
            json.append("\"").append(escapeJsonString((String) value)).append("\"");
        } else if (value instanceof Map) {
            json.append("{\n");
            int index = 0;
            for (Map.Entry<Object, Object> entry : ((Map<Object, Object>) value).entrySet()) {
                if (index > 0) {
                    json.append(",\n");
                }
                json.append("    \"").append(escapeJsonString(entry.getKey().toString())).append("\": ");
                appendJsonValue(json, entry.getValue());
                index++;
            }
            json.append("\n  }");
        } else {
            // Default fallback for other types
            json.append("\"").append(escapeJsonString(value.toString())).append("\"");
        }
    }

    private String escapeJsonString(String str) {
        if (str == null) return "";

        StringBuilder escaped = new StringBuilder();
        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            switch (c) {
                case '"':
                    escaped.append("\\\"");
                    break;
                case '\\':
                    escaped.append("\\\\");
                    break;
                case '\b':
                    escaped.append("\\b");
                    break;
                case '\f':
                    escaped.append("\\f");
                    break;
                case '\n':
                    escaped.append("\\n");
                    break;
                case '\r':
                    escaped.append("\\r");
                    break;
                case '\t':
                    escaped.append("\\t");
                    break;
                default:
                    escaped.append(c);
            }
        }
        return escaped.toString();
    }

    public void setOutputDirectory(String outputDirectory) {
        this.outputDirectory = outputDirectory;
    }

    public void setFileRotateInterval(long fileRotateInterval) {
        this.fileRotateInterval = fileRotateInterval;
    }
}