import java.io.*;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;

public class cleanFile {

        private static final String DATE_FORMAT = "dd/MM/yyyy HH:mm";
        private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(DATE_FORMAT);



        public static void validateAndCleanCSV(String inputFilePath, String outputFilePath) {
            Set<String> uniqueRows = new HashSet<>();
            int lineNumber = 0;
            boolean isValid = true;

            try (BufferedReader reader = Files.newBufferedReader(Paths.get(inputFilePath));
                 BufferedWriter writer = Files.newBufferedWriter(Paths.get(outputFilePath))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    lineNumber++;
                    String[] parts = line.split(",",2);
                    if (parts.length != 2) {
                        System.out.println("שגיאה בשורה " + lineNumber + ": מספר עמודות לא תקין");
                        isValid = false;
                        continue;
                    }

                    String dateStr = parts[0].trim();
                    String valueStr = parts[1].trim();

                    // בדיקת פורמט תאריך
                    if (!isValidDate(dateStr)) {
                        System.out.println("שגיאת פורמט בשורה " + lineNumber + ": " + dateStr);
                        isValid = false;
                        continue;
                    }

                    // בדיקת אם העמודה השנייה מכילה מספר
                    if (!isNumeric(valueStr)) {
                        System.out.println("שגיאת מספר בשורה " + lineNumber + ": " + valueStr);
                        isValid = false;
                        continue;
                    }

                    // בדיקת כפילויות
                    if (!uniqueRows.add(line)) {
                        System.out.println("כפילות בשורה " + lineNumber + ": " + line);
                        isValid = false;
                        continue;
                    }

                    // כתיבת השורה התקינה לקובץ החדש
                    writer.write(line);
                    writer.newLine();
                }
            } catch (IOException e) {
                System.out.println("שגיאה בקריאת הקובץ: " + e.getMessage());
            }

            if (isValid) {
                System.out.println("הקובץ תקין ונשמר כ- " + outputFilePath);
            }
        }

        public static boolean isValidDate(String dateStr) {
            try {
                LocalDateTime.parse(dateStr, formatter);
                return true;
            } catch (DateTimeParseException e) {
                return false;
            }
        }

        public static boolean isNumeric(String str) {
            try {
                double value = Double.parseDouble(str);
                return !Double.isNaN(value);
            } catch (NumberFormatException e) {
                return false;
            }
        }
}
