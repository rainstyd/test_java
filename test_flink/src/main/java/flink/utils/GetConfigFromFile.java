package flink.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class GetConfigFromFile {

    final Properties properties = new Properties();

    // 构造函数，加载配置文件
    public GetConfigFromFile(String[] args) {

        // 加载系统配置文件
        String configFilePath = null;

        // 解析命令行参数
        for (String arg : args) {
            if (arg.startsWith("--config=")) {
                configFilePath = arg.split("=")[1];
                break;
            }
        }

        if (configFilePath == null || configFilePath.isEmpty()) {
            configFilePath = "test_java.properties";
            System.out.println(configFilePath);
        }


        try (InputStream input = getClass().getClassLoader().getResourceAsStream(configFilePath)) {
            if (input == null) {
                throw new IllegalArgumentException("File not found! " + configFilePath);
            }
            // 从输入流加载属性
            properties.load(input);
        } catch (IOException ex) {
            ex.printStackTrace();
            throw new RuntimeException("Failed to load configuration file: " + configFilePath);
        }
    }

    // 获取配置项的方法，使用默认值以防配置项不存在
    public String getProperty(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }

    // 获取配置项的方法，不使用默认值（可能返回null）
    public String getProperty(String key) {
        return properties.getProperty(key);
    }
}
