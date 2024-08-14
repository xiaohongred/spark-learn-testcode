package org.example;

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

public class AppConfig implements Serializable {

    private static final Properties properties = new Properties();

    /**
     * 初始化配置信息
     *
     * @throws IOException IO异常
     */
    public static void initProperties() throws IOException {
        properties.load(AppConfig.class.getClassLoader().getResourceAsStream("application.properties"));
    }

    /**
     * 获得jar中的properties
     *
     * @return 配置信息
     */
    public static Properties getProperties() {
        if (properties.stringPropertyNames().size() == 0) {
            synchronized (properties) {
                if (properties.stringPropertyNames().size() == 0) {
                    try {
                        initProperties();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return properties;
    }
}
