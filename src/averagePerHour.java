import java.io.*;
import java.nio.file.*;
import java.time.*;
import java.time.format.*;
import java.util.*;

public class averagePerHour {

        public static void main(String[] args) {
            Scanner scanner = new Scanner(System.in);
            System.out.print("Please enter the file name or full file path: ");
            String inputFilePath = scanner.nextLine();
            String cleanFileOutput = "cleaned_data.csv";

            cleanFile.validateAndCleanCSV(inputFilePath, cleanFileOutput);
            String resultAvgFile = "output.csv";
            //way 1
//            try {
//                processCSV(cleanFileOutput, resultAvgFile);
//            } catch (IOException e) {
//                System.err.println("Error processing file: " + e.getMessage());
//            }

            // way 2 (best way)
                String outputDirectory = "output_parts";
                String finalOutputFile = "final_output.csv";

                try {
                   splitFileData.splitAndProcessCSV(cleanFileOutput, outputDirectory);
                   splitFileData.mergeProcessedFiles(outputDirectory, finalOutputFile);
                    System.out.println("Processing complete. Final output saved to " + finalOutputFile);
                } catch (IOException e) {
                    System.err.println("Error processing file: " + e.getMessage());
                }

            scanner.close();
        }
    public static void processCSV(String inputFile, String outputFile) throws IOException {
        DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("dd/MM/yyyy H:mm");
        DateTimeFormatter outputFormatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:00");

        Map<String, List<Double>> dataMap = new HashMap<>();

        List<String> lines = Files.readAllLines(Paths.get(inputFile));
        for (String line : lines) {
            String[] parts = line.split(",",2);
            if (parts.length != 2) continue;

            LocalDateTime dateTime = LocalDateTime.parse(parts[0], inputFormatter);
            String roundedHour = dateTime.truncatedTo(java.time.temporal.ChronoUnit.HOURS).format(outputFormatter);
            double value = Double.parseDouble(parts[1]);

            dataMap.computeIfAbsent(roundedHour, k -> new ArrayList<>()).add(value);
        }

        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(outputFile))) {
            writer.write("Start Date,Average\n");
            for (Map.Entry<String, List<Double>> entry : dataMap.entrySet()) {
                double average = entry.getValue().stream().mapToDouble(Double::doubleValue).average().orElse(0);
                writer.write(entry.getKey() + "," + String.format("%.2f", average) + "\n");
            }
        }
        System.out.println("Processing complete. Output saved to " + outputFile);
    }
}




