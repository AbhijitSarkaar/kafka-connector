package com.example.file;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FileSinkConnector extends SinkConnector {

    Map<String, String> configuration;

    // receive task configuration
    @Override
    public void start(Map<String, String> props) {
        this.configuration = props;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return FileSinkTask.class;
    }

    // add configuration to tasks
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = new ArrayList<>();
        configs.add(configuration);
        return configs;
    }

    @Override
    public void stop() {

    }

    @Override
    public ConfigDef config() {
        ConfigDef config = new ConfigDef();

        config.define("topics", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "topic name");
        config.define("file", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "file name");

        return config;
    }

    @Override
    public String version() {
        return "1.0";
    }

}
