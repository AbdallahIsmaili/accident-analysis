package com.usaccidents.io;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * Writes analysis results to JSON files
 */
public class OutputWriter {

    private final String outputDirectory;

    /**
     * Creates a new OutputWriter
     *
     * @param outputDirectory the directory where output files will be written
     */
    public OutputWriter(String outputDirectory) {
        this.outputDirectory = outputDirectory;
    }

    /**
     * Writes analysis results to a JSON file
     *
     * @param results the analysis results to write
     * @throws IOException if an I/O error occurs
     */
    public void writeResults(Map<String, Object> results) throws IOException {
        String timestamp = new SimpleDateFormat("yyyyMMdd-HHmmss").format(new Date());
        File outputFile = new File(outputDirectory, "accidents-analysis-" + timestamp + ".json");

        try (OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(outputFile), StandardCharsets.UTF_8)) {
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

            System.out.println("Results written to: " + outputFile.getAbsolutePath());
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
}