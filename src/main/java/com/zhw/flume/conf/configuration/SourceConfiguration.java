package com.zhw.flume.conf.configuration;

import com.zhw.flume.conf.ComponentConfiguration;
import org.apache.flume.conf.ConfigurationException;

/**
 * @author zhw
 */
public class SourceConfiguration extends ComponentConfiguration {

    protected SourceConfiguration(String componentName) {
        super(componentName);
    }

    public enum SourceConfigurationType {
        OTHER(null);

        private String srcConfigurationName;

        SourceConfigurationType(String src) {
            this.srcConfigurationName = src;
        }

        public String getSourceConfigurationType() {
            return this.srcConfigurationName;
        }

        @SuppressWarnings("unchecked")
        public SourceConfiguration getConfiguration(String name)
                throws ConfigurationException {
            if (this == OTHER) {
                return new SourceConfiguration(name);
            }
            Class<? extends SourceConfiguration> clazz = null;
            SourceConfiguration instance = null;
            try {
                if (srcConfigurationName != null) {
                    clazz =
                            (Class<? extends SourceConfiguration>) Class
                                    .forName(srcConfigurationName);
                    instance = clazz.getConstructor(String.class).newInstance(name);
                } else {
                    // Could not find the configuration stub, do basic validation
                    instance = new SourceConfiguration(name);
                    // Let the caller know that this was created because of this exception.
                    instance.setNotFoundConfigClass();
                }
            } catch (ClassNotFoundException e) {
                // Could not find the configuration stub, do basic validation
                instance = new SourceConfiguration(name);
                // Let the caller know that this was created because of this exception.
                instance.setNotFoundConfigClass();
            } catch (Exception e) {
                throw new ConfigurationException("Error creating configuration", e);
            }
            return instance;
        }
    }
}