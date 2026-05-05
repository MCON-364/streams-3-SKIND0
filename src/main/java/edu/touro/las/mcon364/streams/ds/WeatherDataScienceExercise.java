package edu.touro.las.mcon364.streams.ds;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.util.*;
import java.util.stream.Collectors;


public class WeatherDataScienceExercise {

    public record WeatherRecord(
            String stationId,
            String city,
            String date,
            double temperatureC,
            int humidity,
            double precipitationMm
    ) {}

    public static void main(String[] args) throws Exception {
        List<String> rows = readCsvRows("noaa_weather_sample_200_rows.csv");

        List<WeatherRecord> cleaned = rows.stream()
                .skip(1) // skip header
                .map(WeatherDataScienceExercise::parseRow)
                .flatMap(Optional::stream)
                .filter(WeatherDataScienceExercise::isValid)
                .toList();

        System.out.println("Total raw rows (excluding header): " + (rows.size() - 1));
        System.out.println("Total cleaned rows: " + cleaned.size());

        // TODO 1: Count how many valid weather records remain after cleaning.
        long valid = cleaned.stream().count();
        System.out.println("Total valid rows" + valid);

        // TODO 2: Compute the average temperature across all valid rows.
        OptionalDouble avgTemp = cleaned.stream().mapToDouble(WeatherRecord::temperatureC).average();
        avgTemp.ifPresent(t -> System.out.printf("Average temperature: %.2f%n", t));


        // TODO 3: Find the city with the highest average temperature.
        cleaned.stream().collect(Collectors.groupingBy(WeatherRecord::city, Collectors.averagingDouble(
                WeatherRecord::temperatureC))).entrySet().stream().max(Map.Entry.comparingByValue())
                .ifPresent(e -> System.out.printf("City with highest temperature: %s , %.2f", e.getKey(), e.getValue()));


        // TODO 4: Group records by city.
        Map<String, List<WeatherRecord>> city = cleaned.stream().collect(Collectors.groupingBy(WeatherRecord::city));

        // TODO 5: Compute average precipitation by city.
        Map<String, Double> averageraincity = cleaned.stream().collect(Collectors.groupingBy(WeatherRecord::city,
                Collectors.averagingDouble(WeatherRecord::precipitationMm)));
        System.out.println("Average precipitation by city: " + averageraincity);

        // TODO 6: Partition rows into freezing days (temperature <= 0)
        //  and non-freezing days (temperature > 0).
        Map<Boolean, List<WeatherRecord>> partitioned = cleaned.stream().collect(Collectors.partitioningBy(r -> r.temperatureC() <=0));
        System.out.println("Freezing days: " + partitioned.get(true).size());
        System.out.println("Non freezing days: " + partitioned.get(false).size());

        //missing todo 7 8 and 9. found them in the og one.

        // 7: create a set string of all distinct cities.
        Set<String> distinct = cleaned.stream().map(WeatherRecord::city).collect(Collectors.toSet());
        System.out.println("Distinct cities: " + distinct);

        //8: find the wettest single day.
        cleaned.stream().max(Comparator.comparingDouble(WeatherRecord::precipitationMm))
                .ifPresent(r -> System.out.printf("Wettest day: %s in %s on %s (%.1f mm)%n", r.stationId, r.city, r.date, r.precipitationMm()));

        //9. create a map string, double from city to average humidity
        Map<String, Double> averagehumidity = cleaned.stream().collect(Collectors.groupingBy(WeatherRecord::city, Collectors.averagingDouble(WeatherRecord::humidity)));
        System.out.println("Average humidiity by city: " + averagehumidity);

        // TODO 10: Produce a list of formatted strings like:
        //  "Miami on 2025-01-02: 25.1C, humidity 82%"
        List<String> formatted = cleaned.stream().map(r -> String.format("%s on %s: %.1fC, humidity %d%%", r.city, r.date, r.temperatureC(), r.humidity())).toList();
        formatted.stream().limit(5).forEach(System.out::println);

        // TODO 11 (optional):
        //  Build a Map<String, CityWeatherSummary> for all cities.

        // Put your code below these comments or refactor into helper methods.
    }

    public static Optional<WeatherRecord> parseRow(String row) {
        // TODO:
        // 1. Split the row by commas
        // 2. Reject malformed rows
        // 3. Reject rows with missing temperature
        // 4. Parse numeric values safely
        // 5. Return Optional.empty() if parsing fails

        //throw new UnsupportedOperationException("TODO: implement parseRow");
        String[] parts = row.split(",", -1);
        if (parts.length !=6) return Optional.empty();
        if (parts[3].isEmpty()) return Optional.empty();

        try {
            String stationId = parts[0].trim();
            String city = parts[1].trim();
            String date = parts[2].trim();
            double temperatureC = Double.parseDouble(parts[3].trim());
            int humidity = Integer.parseInt(parts[4].trim());
            double precipitationMm = Double.parseDouble(parts[5].trim());

            return Optional.of(new WeatherRecord(stationId, city, date, temperatureC, humidity, precipitationMm));

        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }

    public static boolean isValid(WeatherRecord r) {
        // TODO:
        // Keep only rows where:
        // - temperature is between -60 and 60
        // - humidity is between 0 and 100
        // - precipitation is >= 0
        return r.temperatureC() >= 60 && r.temperatureC() <= 60
                && r.humidity() >= 0 && r.humidity <=100 && r.precipitationMm() >= 0;

        //throw new UnsupportedOperationException("TODO: implement isValid");
    }

    public record CityWeatherSummary(
            String city,
            long dayCount,
            double avgTemp,
            double avgPrecipitation,
            double maxTemp
    ) {}

    public static List<String> readCsvRows(String fileName) throws IOException {
        InputStream in = WeatherDataScienceExercise.class.getResourceAsStream(fileName);
        if (in == null) {
            throw new NoSuchFileException("Classpath resource not found: " + fileName);
        }
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            return reader.lines().toList();
        }
    }
}
