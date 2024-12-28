package flink.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Properties;

public class GetConfigFromFile implements Serializable {
    private static final long serialVersionUID = 1L;
    final Properties properties = new Properties();

    public GetConfigFromFile(String[] args) {
        String configFilePath = null;

        for (String arg : args) {
            if (arg.startsWith("--config=")) {
                configFilePath = arg.split("=")[1];
                break;
            }
        }

        if (configFilePath == null || configFilePath.isEmpty()) {
            configFilePath = "test_flink.properties";
            System.out.println(configFilePath);
        }

        try (InputStream input = getClass().getClassLoader().getResourceAsStream(configFilePath)) {
            if (input == null) {
                throw new IllegalArgumentException("File not found! " + configFilePath);
            }
            properties.load(input);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to load configuration file: " + configFilePath);
        }
    }

    public String getProperty(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }

    public String getProperty(String key) {
        return properties.getProperty(key);
    }

    public Integer getIntegerProperty(String key, String defaultValue) { return Integer.parseInt(properties.getProperty(key, defaultValue)); }

    public Integer getIntegerProperty(String key) { return Integer.parseInt(properties.getProperty(key)); }

}
