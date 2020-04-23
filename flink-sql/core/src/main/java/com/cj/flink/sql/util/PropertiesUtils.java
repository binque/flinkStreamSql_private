package com.cj.flink.sql.util;

import java.util.Properties;

public class PropertiesUtils {
    public static Properties propertiesTrim(Properties confProperties) {
        Properties properties = new Properties();
        confProperties.forEach(
                (k, v) -> {
                    properties.put(k.toString().trim(), v.toString().trim());
                }
        );
        return properties;
    }
}
