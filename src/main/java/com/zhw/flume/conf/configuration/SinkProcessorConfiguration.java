
package com.zhw.flume.conf.configuration;

import com.zhw.flume.conf.ComponentConfiguration;
import org.apache.flume.Context;
import org.apache.flume.conf.ConfigurationException;

import java.util.Set;

/**
 * @author zhw
 */
public class SinkProcessorConfiguration extends ComponentConfiguration {

    protected Set<String> sinks;

    protected SinkProcessorConfiguration(String componentName) {
        super(componentName);
        setType("default");
    }

    @Override
    public void configure(Context context) throws ConfigurationException {

    }

    public Set<String> getSinks() {
        return sinks;
    }

    public void setSinks(Set<String> sinks) {
        this.sinks = sinks;
    }

    public enum SinkProcessorConfigurationType {

        DEFAULT(null);

        private final String processorClassName;

        private SinkProcessorConfigurationType(String processorClassName) {
            this.processorClassName = processorClassName;
        }

        public String getSinkProcessorConfigurationType() {
            return processorClassName;
        }

        @SuppressWarnings("unchecked")
        public SinkProcessorConfiguration getConfiguration(String name)
                throws ConfigurationException {
            Class<? extends SinkProcessorConfiguration> clazz;
            SinkProcessorConfiguration instance = null;
            try {
                if (processorClassName != null) {
                    clazz = (Class<? extends SinkProcessorConfiguration>) Class
                            .forName(processorClassName);
                    instance = clazz.getConstructor(String.class).newInstance(name);

                } else {
                    return new SinkProcessorConfiguration(name);
                }
            } catch (ClassNotFoundException e) {
                // Could not find the configuration stub, do basic validation
                instance = new SinkProcessorConfiguration(name);
                // Let the caller know that this was created because of this exception.
                instance.setNotFoundConfigClass();
            } catch (Exception e) {
                throw new ConfigurationException("Could not instantiate configuration!", e);
            }
            return instance;
        }
    }
}