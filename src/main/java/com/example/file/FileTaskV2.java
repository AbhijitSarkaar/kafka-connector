package com.example.file;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class FileTaskV2 extends SourceTask {

    private InputStream stream;
    private BufferedReader reader;
    private String topic_name;
    private String file_name;
    private Long streamOffset;
    private static final Schema VALUE_SCHEMA = Schema.STRING_SCHEMA;

    @Override
    public String version() {
        return "2.0";
    }

    @Override
    public void start(Map<String, String> taskConfig) {
        topic_name = taskConfig.get("topic");
        file_name = taskConfig.get("file");
        System.out.println(file_name);
        System.out.println("FileTaskV2.start()");
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        if(stream == null) {
            streamOffset = 0L;
            try {
                System.out.println("poll():: try block");
                stream = new FileInputStream(file_name);
                reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
                String line;
                List<SourceRecord> records = new ArrayList<>();
                while((line = reader.readLine()) != null) {
                    records.add(new SourceRecord(
                            offsetKey(file_name),
                            offsetValue(streamOffset),
                            topic_name,
                            null,
                            null,
                            null,
                            Schema.STRING_SCHEMA,
                            line,
                            System.currentTimeMillis()
                    ));
                    if(streamOffset != null)
                        streamOffset = streamOffset + line.length() + 1; // length of current line including newline character
                }
                return records;
            } catch (FileNotFoundException e) {
            } catch (IOException e) {
            }
            return null;
        }
        return List.of();
    }

    @Override
    public void stop() {
        System.out.println("FileTaskV2.stop()");
    }

    private Map<String, String> offsetKey(String filename) {
        return Collections.singletonMap("filename", filename);
    }

    private Map<String, Long> offsetValue(Long pos) {
        return Collections.singletonMap("position", pos);
    }

}
