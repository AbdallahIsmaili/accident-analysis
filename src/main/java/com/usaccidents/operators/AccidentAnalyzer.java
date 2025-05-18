package com.usaccidents.operators;

import com.usaccidents.model.Accident;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Analyzes accident data to produce various statistics
 */
public class AccidentAnalyzer {
    private static final Logger logger = LoggerFactory.getLogger(AccidentAnalyzer.class);

    private int totalAccidents = 0;
    private Map<String, Integer> accidentsByState = new HashMap<>();
    private Map<Integer, Integer> accidentsBySeverity = new HashMap<>();
    private Map<String, Integer> accidentsByWeatherCondition = new HashMap<>();
    private Map<String, Integer> accidentsByCity = new HashMap<>();
    private Map<Integer, Integer> accidentsByHour = new HashMap<>();

    /**
     * Process a single accident record
     */
    public void processAccident(Accident accident) {
        if (accident == null) {
            return;
        }

        totalAccidents++;

        // Analyze by state
        String state = accident.getState();
        if (state != null && !state.isEmpty()) {
            accidentsByState.put(state, accidentsByState.getOrDefault(state, 0) + 1);
        }

        // Analyze by severity
        int severity = accident.getSeverity();
        accidentsBySeverity.put(severity, accidentsBySeverity.getOrDefault(severity, 0) + 1);

        // Analyze by weather condition
        String weatherCondition = accident.getWeatherCondition();
        if (weatherCondition != null && !weatherCondition.isEmpty()) {
            accidentsByWeatherCondition.put(weatherCondition,
                    accidentsByWeatherCondition.getOrDefault(weatherCondition, 0) + 1);
        }

        // Analyze by city
        String city = accident.getCity();
        if (city != null && !city.isEmpty()) {
            accidentsByCity.put(city, accidentsByCity.getOrDefault(city, 0) + 1);
        }

        // Analyze by hour of day
        LocalDateTime startTime = accident.getStartTime();
        if (startTime != null) {
            int hour = startTime.getHour();
            accidentsByHour.put(hour, accidentsByHour.getOrDefault(hour, 0) + 1);
        }
    }

    /**
     * Get the total number of accidents processed
     */
    public int getTotalAccidents() {
        return totalAccidents;
    }

    /**
     * Get comprehensive analysis results
     */
    public Map<String, Object> getResults() {
        Map<String, Object> results = new HashMap<>();

        results.put("totalAccidents", totalAccidents);

        // Top states by accident count
        results.put("topStatesByAccidentCount", getTopEntries(accidentsByState, 10));

        // Accidents by severity
        results.put("accidentsBySeverity", accidentsBySeverity);

        // Top weather conditions
        results.put("topWeatherConditions", getTopEntries(accidentsByWeatherCondition, 10));

        // Top cities
        results.put("topCitiesByAccidentCount", getTopEntries(accidentsByCity, 20));

        // Accidents by hour
        results.put("accidentsByHour", accidentsByHour);

        // Additional derived statistics
        if (!accidentsBySeverity.isEmpty()) {
            double avgSeverity = accidentsBySeverity.entrySet().stream()
                    .mapToDouble(entry -> entry.getKey() * entry.getValue())
                    .sum() / totalAccidents;
            results.put("averageSeverity", avgSeverity);
        }

        logger.info("Analysis complete. Total accidents processed: {}", totalAccidents);

        return results;
    }

    /**
     * Helper method to get top N entries from a map by value
     */
    private <K> List<Map.Entry<K, Integer>> getTopEntries(Map<K, Integer> map, int n) {
        return map.entrySet().stream()
                .sorted(Map.Entry.<K, Integer>comparingByValue().reversed())
                .limit(n)
                .collect(Collectors.toList());
    }

    /**
     * Reset the analyzer to clear all data
     */
    public void reset() {
        totalAccidents = 0;
        accidentsByState.clear();
        accidentsBySeverity.clear();
        accidentsByWeatherCondition.clear();
        accidentsByCity.clear();
        accidentsByHour.clear();
    }
}