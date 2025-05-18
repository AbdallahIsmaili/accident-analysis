package com.usaccidents.operators;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;
import com.usaccidents.model.Accident;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Operator that analyzes accident data for patterns and insights
 */
public class AccidentAnalyzer extends BaseOperator
{
    private static final Logger LOG = LoggerFactory.getLogger(AccidentAnalyzer.class);

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

    // Windowing parameters
    private long windowDurationMillis = 60000; // Default: 1 minute
    private long lastWindowEmitTime = 0;

    @InputPortFieldAnnotation(optional = false)
    public final transient DefaultInputPort<Accident> input = new DefaultInputPort<Accident>() {
        @Override
        public void process(Accident accident) {
            processAccident(accident);
            checkWindowEmit();
        }
    };

    @OutputPortFieldAnnotation(optional = false)
    public final transient DefaultOutputPort<Map<String, Object>> output = new DefaultOutputPort<>();

    @Override
    public void setup(Context.OperatorContext context) {
        lastWindowEmitTime = System.currentTimeMillis();
    }

    private void processAccident(Accident accident) {
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

    private void checkWindowEmit() {
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastWindowEmitTime >= windowDurationMillis) {
            emitResults();
            lastWindowEmitTime = currentTime;
        }
    }

    private void emitResults() {
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

        // Log and emit the results
        LOG.info("Emitting window results. Total accidents processed: {}", totalAccidents);
        output.emit(results);
    }

    private double calculatePercentage(int part, int total) {
        return (total > 0) ? ((double) part / total) * 100.0 : 0.0;
    }

    public void setWindowDurationMillis(long windowDurationMillis) {
        this.windowDurationMillis = windowDurationMillis;
    }
}