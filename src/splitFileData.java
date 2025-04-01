import java.io.*;
import java.nio.file.*;
import java.time.*;
import java.time.format.*;
import java.util.*;
import java.time.temporal.ChronoUnit;

public class splitFileData {
        public static void splitAndProcessCSV(String inputFile, String outputDirectory) throws IOException {
            DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("dd/MM/yyyy H:mm");
            DateTimeFormatter outputFormatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:00");

            Map<LocalDate, List<String>> dailyData = new HashMap<>();

            List<String> lines = Files.readAllLines(Paths.get(inputFile));
            for (String line : lines) {
                String[] parts = line.split(",",2);
                if (parts.length != 2) continue;

                LocalDateTime dateTime = LocalDateTime.parse(parts[0], inputFormatter);
                dailyData.computeIfAbsent(dateTime.toLocalDate(), k -> new ArrayList<>()).add(line);
            }

            Files.createDirectories(Paths.get(outputDirectory));
            for (Map.Entry<LocalDate, List<String>> entry : dailyData.entrySet()) {
                String dailyFile = outputDirectory + "/" + entry.getKey() + ".csv";
                Files.write(Paths.get(dailyFile), entry.getValue());
                processDailyFile(dailyFile, outputFormatter);
            }
        }

        public static void processDailyFile(String dailyFile, DateTimeFormatter outputFormatter) throws IOException {
            Map<String, List<Double>> hourlyData = new HashMap<>();

            List<String> lines = Files.readAllLines(Paths.get(dailyFile));
            for (String line : lines) {
                String[] parts = line.split(",",2);
                if (parts.length != 2) continue;

                LocalDateTime dateTime = LocalDateTime.parse(parts[0], DateTimeFormatter.ofPattern("dd/MM/yyyy H:mm"));
                String roundedHour = dateTime.truncatedTo(ChronoUnit.HOURS).format(outputFormatter);
                double value = Double.parseDouble(parts[1]);

                hourlyData.computeIfAbsent(roundedHour, k -> new ArrayList<>()).add(value);
            }

            try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(dailyFile))) {
                writer.write("Start Date,Average\n");
                for (Map.Entry<String, List<Double>> entry : hourlyData.entrySet()) {
                    double average = entry.getValue().stream().mapToDouble(Double::doubleValue).average().orElse(0);
                    writer.write(entry.getKey() + "," + String.format("%.2f", average) + "\n");
                }
            }
        }

        public static void mergeProcessedFiles(String inputDirectory, String finalOutputFile) throws IOException {
            try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(finalOutputFile))) {
                writer.write("Start Date,Average\n");
                Files.list(Paths.get(inputDirectory)).sorted().forEach(file -> {
                    try {
                        List<String> lines = Files.readAllLines(file);
                        lines.remove(0); // Remove header
                        for (String line : lines) {
                            writer.write(line + "\n");
                        }
                    } catch (IOException e) {
                        System.err.println("Error merging file: " + file.getFileName());
                    }
                });
            }
        }
    }


