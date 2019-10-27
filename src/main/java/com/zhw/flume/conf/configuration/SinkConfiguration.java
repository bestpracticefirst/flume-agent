
package com.zhw.flume.conf.configuration;

import com.zhw.flume.conf.ComponentConfiguration;
import com.zhw.flume.conf.FlumeConfiguration;
import org.apache.flume.Context;
import org.apache.flume.conf.ConfigurationException;

/**
 * @author zhw
 */
public class SinkConfiguration extends ComponentConfiguration {


    public SinkConfiguration(String componentName) {
        super(componentName);
    }


    @Override
    public void configure(Context context) throws ConfigurationException {
        super.configure(context);
    }

    @Override
    public String toString(int indentCount) {
        String basicStr = super.toString(indentCount);
        StringBuilder sb = new StringBuilder();
        sb.append(basicStr).append(FlumeConfiguration.INDENTSTEP).append(FlumeConfiguration.NEWLINE);
        return sb.toString();
    }

    public enum SinkConfigurationType {

        OTHER(null);

        private final String sinkConfigurationName;

        private SinkConfigurationType(String type) {
            this.sinkConfigurationName = type;
        }

        public String getSinkConfigurationType() {
            return this.sinkConfigurationName;
        }

        @SuppressWarnings("unchecked")
        public SinkConfiguration getConfiguration(String name)
                throws ConfigurationException {
            if (this == OTHER) {
                return new SinkConfiguration(name);
            }
            Class<? extends SinkConfiguration> clazz;
            SinkConfiguration instance = null;
            try {
                if (sinkConfigurationName != null) {
                    clazz = (Class<? extends SinkConfiguration>) Class
                            .forName(sinkConfigurationName);
                    instance = clazz.getConstructor(String.class).newInstance(name);
                } else {
                    return new SinkConfiguration(name);
                }
            } catch (ClassNotFoundException e) {
                // Could not find the configuration stub, do basic validation
                instance = new SinkConfiguration(name);
                // Let the caller know that this was created because of this exception.
                instance.setNotFoundConfigClass();
            } catch (Exception e) {
                throw new ConfigurationException("Couldn't create configuration", e);
            }
            return instance;
        }
    }
}