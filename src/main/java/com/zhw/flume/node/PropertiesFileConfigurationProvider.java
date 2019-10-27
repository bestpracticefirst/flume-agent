
package com.zhw.flume.node;

import com.google.common.collect.Maps;
import com.zhw.flume.conf.FlumeConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author zhw
 */
public class PropertiesFileConfigurationProvider extends AbstractConfigurationProvider {
    private static final Logger LOGGER = LoggerFactory
            .getLogger(PropertiesFileConfigurationProvider.class);

    private final InputStream is;

    public PropertiesFileConfigurationProvider(String agentName, InputStream is) {
        super(agentName);
        this.is = is;
    }

    @Override
    public FlumeConfiguration getFlumeConfiguration() {
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(is));
            Properties properties = new Properties();
            properties.load(reader);
            return new FlumeConfiguration(toMap(properties));
        } catch (IOException ex) {
            LOGGER.error("Unable to load file,  (I/O failure) - Exception follows.", ex);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException ex) {
                    LOGGER.warn(
                            "Unable to close input stream reader for file: ", ex);
                }
            }
        }
        return new FlumeConfiguration(new HashMap<String, String>());
    }

    private Map<String, String> toMap(Properties properties) {
        Map<String, String> result = Maps.newHashMap();
        Enumeration<?> propertyNames = properties.propertyNames();
        while (propertyNames.hasMoreElements()) {
            String name = (String) propertyNames.nextElement();
            String value = properties.getProperty(name);
            result.put(name, value);
        }
        return result;
    }
}