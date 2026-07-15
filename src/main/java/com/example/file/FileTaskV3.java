package com.example.file;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.util.List;
import java.util.Map;

public class FileTaskV3 extends SourceTask {
    @Override
    public String version() {
        return "";
    }

    @Override
    public void start(Map<String, String> props) {

    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        return List.of();
    }

    @Override
    public void stop() {

    }
}
