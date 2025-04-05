package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.OriginalType;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

public class ParquetReaderAndClean {
    public static void main() throws IOException {
        String parquetFilePath = "time_series.parquet";
        String outputParquetFilePath = "filtered_output.parquet";

        Configuration conf = new Configuration();
        Path path = new Path(parquetFilePath);
        Path outputPath = new Path(outputParquetFilePath);


        MessageType schema = Types.buildMessage()
                .required(PrimitiveTypeName.INT32).named("timestamp")
                .required(PrimitiveTypeName.DOUBLE).named("mean_value")
                .required(PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("sensor_id")
                .named("parquet_schema");


        GroupWriteSupport.setSchema(schema, conf);


        ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), path)
                .withConf(conf)
                .build();


        ParquetWriter<Group> writer = new ParquetWriter<>(
                outputPath,
                new GroupWriteSupport(),
                ParquetWriter.DEFAULT_COMPRESSION_CODEC_NAME,
                ParquetWriter.DEFAULT_BLOCK_SIZE,
                ParquetWriter.DEFAULT_PAGE_SIZE
        );


        Set<String> seenDateHourSet = new HashSet<>();

        Group group;
        while ((group = reader.read()) != null) {
            long timestampNanoseconds = group.getLong("timestamp", 0);
            long timestampMilliseconds = timestampNanoseconds / 1_000_000;
            Date date = new Date(timestampMilliseconds);


            SimpleDateFormat fullFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm");
            String formattedDate = fullFormat.format(date);


            SimpleDateFormat hourKeyFormat = new SimpleDateFormat("dd/MM/yyyy HH");
            String dateHourKey = hourKeyFormat.format(date);

            if (isValidDate(formattedDate) && !seenDateHourSet.contains(dateHourKey)) {
                int meanValueIndex = group.getType().getFieldIndex("mean_value");

                String meanValueStr = group.getValueToString(meanValueIndex, 0);

                if (isNumber(meanValueStr)) {
                    seenDateHourSet.add(dateHourKey);

                    writer.write(group);
                }
            }
        }

        reader.close();
        writer.close();
    }

    private static boolean isValidDate(String dateStr) {
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm");
        sdf.setLenient(false);
        try {
            sdf.parse(dateStr);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static boolean isNumber(String str) {
        try {
            Double.parseDouble(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
    public static List<Group> readParquet(String filePath) throws IOException {
        List<Group> validRows = new ArrayList<>();

        Configuration configuration = new Configuration();
        Path path = new Path(filePath);
        ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), path).withConf(configuration).build();

        Group group;
        while ((group = reader.read()) != null) {
            validRows.add(group);
        }

        reader.close();
        return validRows;
    }
}
