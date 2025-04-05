package org.example;

import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.example.data.Group;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Main {
    private static final String OUTPUT_FILE = "output_with_averages.parquet";
    private static final String DATE_FORMAT = "dd/MM/yyyy HH:mm";

    public static void main(String[] args) throws IOException {
       String cleanParquetFile="filtered_output.parquet";
        getFilteredData();
        List<Group> validRows = ParquetReaderAndClean.readParquet(cleanParquetFile);

        Map<String, List<Double>> groupedData = new HashMap<>();


        for (Group group : validRows) {
            String timestamp = group.getString("timestamp", 0);
            String meanValueStr = group.getString("mean_value", 0);


            String hour = getRoundedHour(timestamp);
            double meanValue = Double.parseDouble(meanValueStr);

            groupedData.computeIfAbsent(hour, k -> new ArrayList<>()).add(meanValue);
        }


        Map<String, Double> averagePerHour = new HashMap<>();
        for (Map.Entry<String, List<Double>> entry : groupedData.entrySet()) {
            double average = entry.getValue().stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
            averagePerHour.put(entry.getKey(), average);
        }


        String schemaStr = "message schema {"
                + " required binary timestamp (UTF8);"
                + " required double mean_value;"
                + "}";
        MessageType schema = MessageTypeParser.parseMessageType(schemaStr);


        Configuration conf = new Configuration();
        GroupWriteSupport.setSchema(schema, conf);
        Path outputPath = new Path(OUTPUT_FILE);


        ParquetWriter<Group> writer = new ParquetWriter<>(outputPath, new GroupWriteSupport(), CompressionCodecName.SNAPPY, ParquetWriter.DEFAULT_BLOCK_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE, true, false);


        for (Map.Entry<String, Double> entry : averagePerHour.entrySet()) {
            Group group = new SimpleGroup(schema);
            group.add("timestamp", entry.getKey());
            group.add("mean_value", entry.getValue());
            writer.write(group);
        }


        writer.close();

        System.out.println("Processing complete! written to " + OUTPUT_FILE);
    }


    private static String getRoundedHour(String timestamp) {
        SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
        try {
            Date date = sdf.parse(timestamp);
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(date);


            int minute = calendar.get(Calendar.MINUTE);
            if (minute > 0) {
                calendar.add(Calendar.HOUR_OF_DAY, -1);
            }


            return String.format("%02d", calendar.get(Calendar.HOUR_OF_DAY));
        } catch (Exception e) {
            return "";
        }
    }


    private static void getFilteredData() {
        try {
            ParquetReaderAndClean.main();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}
