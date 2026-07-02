package com.example.file;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.io.BufferedReader;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

public class FileTaskV2 extends SourceTask {

    private InputStream stream;
    private BufferedReader reader;
    private String topic_name;
    private String file_name;

    @Override
    public String version() {
        return "2.0";
    }

    @Override
    public void start(Map<String, String> taskConfig) {
        topic_name = taskConfig.get("topic");
        file_name = taskConfig.get("file_name");
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        return List.of();
    }

    @Override
    public void stop() {

    }
}
