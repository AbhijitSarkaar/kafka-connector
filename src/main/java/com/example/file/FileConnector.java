package com.example.file;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FileConnector extends SourceConnector {

    private Map<String, String> props;

    @Override
    public void start(Map<String, String> props) {
        // receive configuration during rest api call to create connector
        this.props = props;
        AbstractConfig config = new AbstractConfig(config(), props);
        String filename = config.getString("file");
        System.out.println("file name: " + filename);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return FileTaskV2.class;
    }

    // returns a list of configs
    // worker creates a task for each config
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        // passing received configuration to tasks
        configs.add(props);
        return configs;
    }

    @Override
    public void stop() {
        System.out.println("FileConnector.stop()");
    }

    @Override
    public ConfigDef config() {
        ConfigDef configDef = new ConfigDef();

        configDef.define("topic", ConfigDef.Type.STRING, Importance.HIGH, "topic name");
        configDef.define("tasks.max", ConfigDef.Type.INT, Importance.HIGH, "number of tasks");
        configDef.define("file", ConfigDef.Type.STRING, Importance.HIGH, "source file name");

        return configDef;
    }

    @Override
    public String version() {
        return "1.0";
    }
}
