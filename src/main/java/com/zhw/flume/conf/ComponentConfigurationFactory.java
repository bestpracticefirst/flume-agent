
package com.zhw.flume.conf;

import com.zhw.flume.conf.configuration.SinkConfiguration;
import com.zhw.flume.conf.configuration.SinkGroupConfiguration;
import com.zhw.flume.conf.configuration.SinkProcessorConfiguration;
import com.zhw.flume.conf.configuration.SourceConfiguration;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.conf.configfilter.ConfigFilterConfiguration;

import java.util.Locale;

/**
 * @author zhw
 */
public class ComponentConfigurationFactory {

    @SuppressWarnings("unchecked")
    public static ComponentConfiguration create(String name, String type, ComponentConfiguration.ComponentType component)
            throws ConfigurationException {
        Class<? extends ComponentConfiguration> confType = null;

        if (type == null) {
            throw new ConfigurationException(
                    "Cannot create component without knowing its type!");
        }
        try {
            confType = (Class<? extends ComponentConfiguration>) Class.forName(type);
            return confType.getConstructor(String.class).newInstance(type);
        } catch (Exception ignored) {
            try {
                type = type.toUpperCase(Locale.ENGLISH);
                switch (component) {
                    case SOURCE:
                        return SourceConfiguration.SourceConfigurationType.valueOf(type.toUpperCase(Locale.ENGLISH))
                                .getConfiguration(name);
                    case SINK:
                        return SinkConfiguration.SinkConfigurationType.valueOf(type.toUpperCase(Locale.ENGLISH))
                                .getConfiguration(name);
                    case SINK_PROCESSOR:
                        return SinkProcessorConfiguration.SinkProcessorConfigurationType.valueOf(type.toUpperCase(Locale.ENGLISH))
                                .getConfiguration(name);

                    case SINKGROUP:
                        return new SinkGroupConfiguration(name);
                    default:
                        throw new ConfigurationException(
                                "Cannot create configuration. Unknown Type specified: " + type);
                }
            } catch (ConfigurationException e) {
                throw e;
            } catch (Exception e) {
                throw new ConfigurationException("Could not create configuration! " +
                        " Due to " + e.getClass().getSimpleName() + ": " + e.getMessage(),
                        e);
            }
        }
    }
}