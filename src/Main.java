import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.regex.*;

public class Main {
    private static final int MAX_LINES_PER_PART = 100000;

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        System.out.print("Please enter the file name or full file path: ");
        String fileName = scanner.nextLine();
        System.out.print("How many of the most common errors would you like to see? ");
        int n=scanner.nextInt();

        int totalLines=countLines(fileName);
        int numberOfParts = decideNumberOfParts(totalLines);
        splitLogFile(fileName);

        List<String> partFiles = new ArrayList<>();
        for (int i = 1; i <= numberOfParts; i++) {
            partFiles.add("split/log_part_" + i + ".txt");
        }

        Map<Integer, Integer> mergedCounts = mergeErrorCounts(partFiles);

        List<Map.Entry<Integer, Integer>> topErrors = getTopErrors(mergedCounts, n);

        System.out.println("Top "+n+" error codes:");
        for (Map.Entry<Integer, Integer> entry : topErrors) {
            System.out.println("Error Code: " + entry.getKey() + " - Count: " + entry.getValue());
        }
        scanner.close();
        //O(n log k)
    }

    //O(n)
    public static int countLines(String fileName) {
        int lines = 0;
        try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            while (reader.readLine() != null) {
                lines++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return lines;
    }
    //O(1)
    public static int decideNumberOfParts(int totalLines) {

        return (int) Math.ceil((double) totalLines / MAX_LINES_PER_PART);
    }

    //O(n)
    public static void splitLogFile(String inputFile) {
        File dir = new File("split");
        if (!dir.exists()) {
            dir.mkdir();
        }
        try (BufferedReader reader = new BufferedReader(new FileReader(inputFile))) {
            String line;
            int partNumber = 1;
            int lineCount = 0;
            PrintWriter writer = new PrintWriter(new FileWriter("split/log_part_" + partNumber + ".txt"));

            while ((line = reader.readLine()) != null) {
                writer.println(line);
                lineCount++;

                if (lineCount >= MAX_LINES_PER_PART) {
                    writer.close();
                    partNumber++;
                    writer = new PrintWriter(new FileWriter("split/log_part_" + partNumber + ".txt"));
                    lineCount = 0;
                }
            }
            writer.close();
            System.out.println("Splitting completed into " + partNumber + " parts.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    //O(n`)
    public static Map<Integer, Integer> countErrorsInFile(String fileName) {
        Map<Integer, Integer> errorCount = new HashMap<>();
        Pattern pattern = Pattern.compile("Error: \\w+_(\\d+)");
        try {
            List<String> lines = Files.readAllLines(Paths.get(fileName));
            for (String line : lines) {
                Matcher matcher = pattern.matcher(line);
                if (matcher.find()) {
                    int errorCode = Integer.parseInt(matcher.group(1));
                    errorCount.put(errorCode, errorCount.getOrDefault(errorCode, 0) + 1);
                }
            }
        }catch (IOException e) {
            e.printStackTrace();
        }
        return errorCount;
    }
    //O(n)
    public static Map<Integer, Integer> mergeErrorCounts(List<String> partFiles) {
        Map<Integer, Integer> globalErrorCount = new HashMap<>();


        for (String file : partFiles) {
            Map<Integer, Integer> partCount = countErrorsInFile(file);
            for (Map.Entry<Integer, Integer> entry : partCount.entrySet()) {
                globalErrorCount.put(entry.getKey(), globalErrorCount.getOrDefault(entry.getKey(), 0) + entry.getValue());
            }
        }
        return globalErrorCount;
    }
    //O(k log k)
    public static List<Map.Entry<Integer, Integer>> getTopErrors(Map<Integer, Integer> errorCounts, int N) {
        List<Map.Entry<Integer, Integer>> sortedErrors = new ArrayList<>(errorCounts.entrySet());
        sortedErrors.sort((a, b) -> b.getValue() - a.getValue());

        return sortedErrors.subList(0, Math.min(N, sortedErrors.size()));
    }


}