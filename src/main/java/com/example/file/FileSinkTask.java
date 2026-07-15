package com.example.file;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public class FileSinkTask extends SinkTask {

    private FileWriter fileWriter;
    private BufferedWriter bufferedWriter;
    private Map<String, String> config;

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> props) {
        System.out.println("FileSinkTask.start()");
        this.config = props;
        System.out.println(this.config.get("topic"));
        System.out.println(this.config.get("file"));
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        System.out.println("FileSinkTask.put()");
        System.out.println("FileSinkTask.put() records.size()" + records.size());
        System.out.println("File to write to " + this.config.get("file"));

        try {

            fileWriter = new FileWriter(this.config.get("file"));
            bufferedWriter = new BufferedWriter(fileWriter);

            for(SinkRecord record: records) {
                System.out.println("SinkRecord: record.value().toString() " + record.value().toString());
                bufferedWriter.write(record.value().toString());
                bufferedWriter.newLine();
            }

            bufferedWriter.close();

        } catch (IOException e) {
        }
    }

    @Override
    public void stop() {

    }

}
