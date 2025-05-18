package com.usaccidents.operators;

import com.usaccidents.model.Accident;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Analyzes accident data to find patterns and insights
 */
public class AccidentAnalyzer {

    // Analysis metrics
    private Map<String, Integer> accidentsByState = new ConcurrentHashMap<>();
    private Map<String, Integer> accidentsByWeatherCondition = new ConcurrentHashMap<>();
    private Map<Integer, Integer> accidentsBySeverity = new ConcurrentHashMap<>();
    private Map<String, Integer> accidentsByTimeOfDay = new ConcurrentHashMap<>();
    private Map<String, Integer> accidentsByDayOfWeek = new ConcurrentHashMap<>();
    private int totalAccidents = 0;
    private int nightAccidents = 0;
    private int badWeatherAccidents = 0;
    private int urbanAreaAccidents = 0;
    private int intersectionAccidents = 0;

    /**
     * Processes a single accident record
     *
     * @param accident the accident to process
     */
    public void processAccident(Accident accident) {
        if (accident == null) return;

        totalAccidents++;

        // Analyze by state
        incrementMapCount(accidentsByState, accident.getState());

        // Analyze by weather condition
        incrementMapCount(accidentsByWeatherCondition, accident.getWeatherCondition());

        // Analyze by severity
        incrementMapCount(accidentsBySeverity, accident.getSeverity());

        // Analyze by time of day (hour)
        int hour = accident.getHourOfDay();
        if (hour >= 0) {
            String timeBlock;
            if (hour < 6) timeBlock = "Night (0-6)";
            else if (hour < 12) timeBlock = "Morning (6-12)";
            else if (hour < 18) timeBlock = "Afternoon (12-18)";
            else timeBlock = "Evening (18-24)";
            incrementMapCount(accidentsByTimeOfDay, timeBlock);
        }

        // Analyze by day of week
        incrementMapCount(accidentsByDayOfWeek, accident.getDayOfWeek());

        // Special cases
        if (accident.isNightAccident()) {
            nightAccidents++;
        }

        if (accident.hasBadWeather()) {
            badWeatherAccidents++;
        }

        if (accident.isUrbanArea()) {
            urbanAreaAccidents++;
        }

        if (accident.isAtIntersection()) {
            intersectionAccidents++;
        }
    }

    private <K> void incrementMapCount(Map<K, Integer> map, K key) {
        if (key == null) return;
        map.compute(key, (k, v) -> (v == null) ? 1 : v + 1);
    }

    /**
     * Returns the current analysis results
     *
     * @return a map containing all the analysis results
     */
    public Map<String, Object> getResults() {
        Map<String, Object> results = new HashMap<>();

        // Add all metrics to results
        results.put("totalAccidents", totalAccidents);
        results.put("accidentsByState", new TreeMap<>(accidentsByState));
        results.put("accidentsByWeatherCondition", new TreeMap<>(accidentsByWeatherCondition));
        results.put("accidentsBySeverity", new TreeMap<>(accidentsBySeverity));
        results.put("accidentsByTimeOfDay", accidentsByTimeOfDay);
        results.put("accidentsByDayOfWeek", accidentsByDayOfWeek);
        results.put("nightAccidentsCount", nightAccidents);
        results.put("nightAccidentsPercentage", calculatePercentage(nightAccidents, totalAccidents));
        results.put("badWeatherAccidentsCount", badWeatherAccidents);
        results.put("badWeatherAccidentsPercentage", calculatePercentage(badWeatherAccidents, totalAccidents));
        results.put("urbanAreaAccidentsCount", urbanAreaAccidents);
        results.put("urbanAreaAccidentsPercentage", calculatePercentage(urbanAreaAccidents, totalAccidents));
        results.put("intersectionAccidentsCount", intersectionAccidents);
        results.put("intersectionAccidentsPercentage", calculatePercentage(intersectionAccidents, totalAccidents));

        return results;
    }

    private double calculatePercentage(int part, int total) {
        return (total > 0) ? ((double) part / total) * 100.0 : 0.0;
    }

    /**
     * Returns the total number of accidents processed
     *
     * @return the total number of accidents
     */
    public int getTotalAccidents() {
        return totalAccidents;
    }
}