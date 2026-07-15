package com.example.file;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class FileConnector2 extends SourceConnector {

    Map<String, String> props;

    @Override
    public void start(Map<String, String> props) {
        this.props = props;
        System.out.println(this.props.get("topic_name"));
    }

    @Override
    public Class<? extends Task> taskClass() {
        return FileTaskV2.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();

        // total of one task
        configs.add(this.props);

        return configs;
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        ConfigDef config = new ConfigDef();

        config.define("topic", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "topic name");
        config.define("file", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "file name");

        return config;
    }

    @Override
    public String version() {
        return "2.0";
    }

}
