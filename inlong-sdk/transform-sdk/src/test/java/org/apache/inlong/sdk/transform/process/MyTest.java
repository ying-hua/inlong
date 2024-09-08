package org.apache.inlong.sdk.transform.process;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MyTest {
    static String schemaString = "{"
            + "\"namespace\": \"test\","
            + "\"type\": \"record\","
            + "\"name\": \"SdkDataRequest\","
            + "\"fields\": ["
            +   "{"
            +     "\"name\": \"sid\","
            +     "\"type\": \"string\""
            +   "},"
            +   "{"
            +     "\"name\": \"msgs\","
            +     "\"type\": {"
            +       "\"type\": \"array\","
            +       "\"items\": {"
            +         "\"type\": \"record\","
            +         "\"name\": \"SdkMessage\","
            +         "\"fields\": ["
            +           "{"
            +             "\"name\": \"msg\","
            +             "\"type\": \"bytes\""
            +           "},"
            +           "{"
            +             "\"name\": \"msgTime\","
            +             "\"type\": \"long\""
            +           "},"
            +           "{"
            +             "\"name\": \"extinfo\","
            +             "\"type\": {"
            +               "\"type\": \"map\","
            +               "\"values\": \"string\""
            +             "}"
            +           "}"
            +         "]"
            +       "}"
            +     "}"
            +   "},"
            +   "{"
            +     "\"name\": \"packageID\","
            +     "\"type\": \"long\""
            +   "}"
            + "]}";
    public static void main(String[] args) throws Exception {
        //createAvroFile("inlong-sdk/transform-sdk/src/test/java/org/apache/inlong/sdk/transform/process/test.avro");
        readAvroFile("inlong-sdk/transform-sdk/src/test/java/org/apache/inlong/sdk/transform/process/test.avro");
    }

    public static void createAvroFile(String filePath) throws Exception {

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(schemaString);

        GenericRecord record = new GenericData.Record(schema);
        record.put("sid", "some-id");

        List<GenericRecord> messages = new ArrayList<>();
        Schema messageSchema = schema.getField("msgs").schema().getElementType();
        GenericRecord message1 = new GenericData.Record(messageSchema);
        message1.put("msg", ByteBuffer.wrap("Hello World".getBytes(StandardCharsets.UTF_8)));
        message1.put("msgTime", 1679493968L);
        Map<String, String> extinfo = new HashMap<>();
        extinfo.put("key1", "value1");
        extinfo.put("key2", "value2");
        message1.put("extinfo", extinfo);

        messages.add(message1);

        record.put("msgs", messages);
        record.put("packageID", 123456L);

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);

        File outputFile = new File(filePath);
        dataFileWriter.create(schema, outputFile);
        dataFileWriter.append(record);
        dataFileWriter.close();
    }

    public static void readAvroFile(String filePath) throws Exception {
        File file = new File(filePath);
        byte[] bytes = Files.readAllBytes(Paths.get(filePath));
        InputStream inputStream = new ByteArrayInputStream(bytes);
        // 创建一个DataFileStream来读取Avro数据
        DataFileStream<GenericRecord> dataFileStream = new DataFileStream<>(inputStream, new GenericDatumReader<>());

        for (GenericRecord record : dataFileStream) {
            System.out.println("Record: " + record.toString());

            String sid = record.get("sid").toString();
            long packageID = (Long) record.get("packageID");

            @SuppressWarnings("unchecked")
            List<GenericRecord> msgs = (List<GenericRecord>) record.get("msgs");

            for (GenericRecord msg : msgs) {
                ByteBuffer msgBuffer = (ByteBuffer) msg.get("msg");
                byte[] msgBytes = new byte[msgBuffer.remaining()];
                msgBuffer.get(msgBytes);
                long msgTime = (Long) msg.get("msgTime");
                Map<String, String> extinfo = (Map<String, String>) msg.get("extinfo");

                System.out.println("SID: " + sid);
                System.out.println("Package ID: " + packageID);
                System.out.println("Message: " + new String(msgBytes));
                System.out.println("Message Time: " + msgTime);
                System.out.println("Extinfo: " + extinfo);
            }
        }

        dataFileStream.close();
    }
}