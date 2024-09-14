/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.sdk.transform.process;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class readAvroFile {

    static String schemaString = "{\n" +
            "  \"namespace\": \"test\",\n" +
            "  \"type\": \"record\",\n" +
            "  \"name\": \"SdkDataRequest\",\n" +
            "  \"fields\": [\n" +
            "    {\n" +
            "      \"name\": \"sid\",\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"msgs\",\n" +
            "      \"type\": {\n" +
            "        \"type\": \"array\",\n" +
            "        \"items\": {\n" +
            "          \"type\": \"record\",\n" +
            "          \"name\": \"SdkMessage\",\n" +
            "          \"fields\": [\n" +
            "            {\n" +
            "              \"name\": \"msg\",\n" +
            "              \"type\": \"bytes\"\n" +
            "            },\n" +
            "            {\n" +
            "              \"name\": \"msgTime\",\n" +
            "              \"type\": \"long\"\n" +
            "            },\n" +
            "            {\n" +
            "              \"name\": \"extinfo\",\n" +
            "              \"type\": {\n" +
            "                \"type\": \"map\",\n" +
            "                \"values\": \"string\"\n" +
            "              }\n" +
            "            }\n" +
            "          ]\n" +
            "        }\n" +
            "      }\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"packageID\",\n" +
            "      \"type\": \"long\"\n" +
            "    }\n" +
            "  ]\n" +
            "}";
    public static void main(String[] args) throws Exception {
        // createAvroFile("inlong-sdk/transform-sdk/src/test/java/org/apache/inlong/sdk/transform/process/test.avro");
        readAvroFile("inlong-sdk/transform-sdk/src/test/java/org/apache/inlong/sdk/transform/process/test.avro");
    }

    public static void createAvroFile(String filePath) throws Exception {

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(schemaString);

        GenericRecord record = new GenericData.Record(schema);
        record.put("sid", "sid1");

        List<GenericRecord> messages = new ArrayList<>();
        Schema messageSchema = schema.getField("msgs").schema().getElementType();
        GenericRecord message1 = new GenericData.Record(messageSchema);
        message1.put("msg", ByteBuffer.wrap("Apple".getBytes(StandardCharsets.UTF_8)));
        message1.put("msgTime", 10011001L);
        Map<String, String> extinfo = new HashMap<>();
        extinfo.put("key1", "key1");
        extinfo.put("key2", "value1");
        message1.put("extinfo", extinfo);

        GenericRecord message2 = new GenericData.Record(messageSchema);
        message2.put("msg", ByteBuffer.wrap("Banana".getBytes(StandardCharsets.UTF_8)));
        message2.put("msgTime", 20022002L);
        extinfo = new HashMap<>();
        extinfo.put("key1", "key2");
        extinfo.put("key2", "value2");
        message2.put("extinfo", extinfo);

        messages.add(message1);
        messages.add(message2);

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
        System.out.println(Base64.getEncoder().encodeToString(bytes));
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
                for (Object key : extinfo.keySet()) {
                    System.out.println(key.getClass());
                }
                System.out.println("SID: " + sid);
                System.out.println("Package ID: " + packageID);
                System.out.println("Message: " + new String(msgBytes));
                System.out.println("Message Time: " + msgTime);
                System.out.println("Extinfo: " + extinfo);
                System.out.println(extinfo.keySet());
                List<String> keys = new ArrayList<>(extinfo.keySet());
            }
        }

        dataFileStream.close();
    }
}