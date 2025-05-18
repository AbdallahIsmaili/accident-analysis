package com.usaccidents.operators;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;
import com.usaccidents.model.Accident;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AccidentCSVParser extends BaseOperator
{
    private static final Logger LOG = LoggerFactory.getLogger(AccidentCSVParser.class);

    // Expected CSV headers
    private static final String[] EXPECTED_HEADERS = {
            "ID", "Source", "Severity", "Start_Time", "End_Time", "Start_Lat", "Start_Lng",
            "Distance(mi)", "Description", "Street", "City", "County", "State", "Zipcode",
            "Country", "Timezone", "Airport_Code", "Temperature(F)", "Wind_Chill(F)",
            "Humidity(%)", "Pressure(in)", "Visibility(mi)", "Wind_Direction", "Wind_Speed(mph)",
            "Precipitation(in)", "Weather_Condition", "Amenity", "Bump", "Crossing", "Give_Way",
            "Junction", "No_Exit", "Railway", "Roundabout", "Station", "Stop", "Traffic_Calming",
            "Traffic_Signal", "Turning_Loop", "Sunrise_Sunset", "Civil_Twilight", "Nautical_Twilight",
            "Astronomical_Twilight", "Weather_Timestamp_Filled"
    };

    private boolean headersProcessed = false;
    private int[] columnIndices = new int[EXPECTED_HEADERS.length];

    @InputPortFieldAnnotation(optional = false)
    public final transient DefaultInputPort<String> input = new DefaultInputPort<String>() {
        @Override
        public void process(String line) {
            processCsvLine(line);
        }
    };

    @OutputPortFieldAnnotation(optional = false)
    public final transient DefaultOutputPort<Accident> output = new DefaultOutputPort<>();

    private void processCsvLine(String line) {
        if (line == null || line.trim().isEmpty()) {
            return;
        }

        String[] fields = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);

        // Process headers if this is the first line
        if (!headersProcessed) {
            processHeaders(fields);
            headersProcessed = true;
            return;
        }

        try {
            Accident accident = parseAccident(fields);
            if (accident != null) {
                output.emit(accident);
            }
        } catch (Exception e) {
            LOG.error("Error parsing CSV line: " + line, e);
        }
    }

    private void processHeaders(String[] headers) {
        // Initialize all indices to -1 (not found)
        for (int i = 0; i < columnIndices.length; i++) {
            columnIndices[i] = -1;
        }

        // Map the actual header columns to our expected columns
        for (int i = 0; i < headers.length; i++) {
            String header = headers[i].trim().replace("\"", "");
            for (int j = 0; j < EXPECTED_HEADERS.length; j++) {
                if (EXPECTED_HEADERS[j].equalsIgnoreCase(header)) {
                    columnIndices[j] = i;
                    break;
                }
            }
        }

        // Log missing columns
        for (int i = 0; i < EXPECTED_HEADERS.length; i++) {
            if (columnIndices[i] == -1) {
                LOG.warn("Column not found in CSV: " + EXPECTED_HEADERS[i]);
            }
        }
    }

    private Accident parseAccident(String[] fields) {
        Accident accident = new Accident();

        try {
            // ID is mandatory
            int idIdx = columnIndices[0];
            if (idIdx >= 0 && idIdx < fields.length) {
                accident.setId(cleanField(fields[idIdx]));
            } else {
                return null; // Skip if no ID
            }

            // Set remaining fields
            setStringField(accident::setSource, fields, columnIndices[1]);
            setIntField(accident::setSeverity, fields, columnIndices[2]);
            setStringField(accident::setStartTime, fields, columnIndices[3]);
            setStringField(accident::setEndTime, fields, columnIndices[4]);
            setDoubleField(accident::setStartLat, fields, columnIndices[5]);
            setDoubleField(accident::setStartLng, fields, columnIndices[6]);
            setDoubleField(accident::setDistanceMi, fields, columnIndices[7]);
            setStringField(accident::setDescription, fields, columnIndices[8]);
            setStringField(accident::setStreet, fields, columnIndices[9]);
            setStringField(accident::setCity, fields, columnIndices[10]);
            setStringField(accident::setCounty, fields, columnIndices[11]);
            setStringField(accident::setState, fields, columnIndices[12]);
            setStringField(accident::setZipcode, fields, columnIndices[13]);
            setStringField(accident::setCountry, fields, columnIndices[14]);
            setStringField(accident::setTimezone, fields, columnIndices[15]);
            setStringField(accident::setAirportCode, fields, columnIndices[16]);
            setDoubleField(accident::setTemperatureF, fields, columnIndices[17]);
            setDoubleField(accident::setWindChillF, fields, columnIndices[18]);
            setDoubleField(accident::setHumidityPercent, fields, columnIndices[19]);
            setDoubleField(accident::setPressureIn, fields, columnIndices[20]);
            setDoubleField(accident::setVisibilityMi, fields, columnIndices[21]);
            setStringField(accident::setWindDirection, fields, columnIndices[22]);
            setDoubleField(accident::setWindSpeedMph, fields, columnIndices[23]);
            setDoubleField(accident::setPrecipitationIn, fields, columnIndices[24]);
            setStringField(accident::setWeatherCondition, fields, columnIndices[25]);
            setStringField(accident::setAmenity, fields, columnIndices[26]);
            setStringField(accident::setBump, fields, columnIndices[27]);
            setStringField(accident::setCrossing, fields, columnIndices[28]);
            setStringField(accident::setGiveWay, fields, columnIndices[29]);
            setStringField(accident::setJunction, fields, columnIndices[30]);
            setStringField(accident::setNoExit, fields, columnIndices[31]);
            setStringField(accident::setRailway, fields, columnIndices[32]);
            setStringField(accident::setRoundabout, fields, columnIndices[33]);
            setStringField(accident::setStation, fields, columnIndices[34]);
            setStringField(accident::setStop, fields, columnIndices[35]);
            setStringField(accident::setTrafficCalming, fields, columnIndices[36]);
            setStringField(accident::setTrafficSignal, fields, columnIndices[37]);
            setStringField(accident::setTurningLoop, fields, columnIndices[38]);
            setStringField(accident::setSunriseSunset, fields, columnIndices[39]);
            setStringField(accident::setCivilTwilight, fields, columnIndices[40]);
            setStringField(accident::setNauticalTwilight, fields, columnIndices[41]);
            setStringField(accident::setAstronomicalTwilight, fields, columnIndices[42]);
            setStringField(accident::setWeatherTimestampFilled, fields, columnIndices[43]);

            return accident;
        } catch (Exception e) {
            LOG.error("Error parsing accident data", e);
            return null;
        }
    }

    private String cleanField(String field) {
        if (field == null) return null;
        return field.trim().replace("\"", "");
    }

    private void setStringField(java.util.function.Consumer<String> setter, String[] fields, int index) {
        if (index >= 0 && index < fields.length) {
            setter.accept(cleanField(fields[index]));
        }
    }

    private void setIntField(java.util.function.IntConsumer setter, String[] fields, int index) {
        if (index >= 0 && index < fields.length) {
            String value = cleanField(fields[index]);
            if (value != null && !value.isEmpty()) {
                try {
                    setter.accept(Integer.parseInt(value));
                } catch (NumberFormatException e) {
                    LOG.warn("Invalid integer value: " + value);
                }
            }
        }
    }

    private void setDoubleField(java.util.function.DoubleConsumer setter, String[] fields, int index) {
        if (index >= 0 && index < fields.length) {
            String value = cleanField(fields[index]);
            if (value != null && !value.isEmpty()) {
                try {
                    setter.accept(Double.parseDouble(value));
                } catch (NumberFormatException e) {
                    LOG.warn("Invalid double value: " + value);
                }
            }
        }
    }
}