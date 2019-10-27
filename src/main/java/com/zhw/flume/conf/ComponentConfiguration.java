
package com.zhw.flume.conf;

import org.apache.flume.Context;
import org.apache.flume.conf.BasicConfigurationConstants;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.conf.FlumeConfigurationError;
import org.apache.flume.conf.FlumeConfigurationErrorType;

import java.util.LinkedList;
import java.util.List;

/**
 * @author zhw
 */
public class ComponentConfiguration {

    protected String componentName;

    private String type;
    protected boolean configured;
    protected List<FlumeConfigurationError> errors;
    private boolean notFoundConfigClass;

    public boolean isNotFoundConfigClass() {
        return notFoundConfigClass;
    }

    public void setNotFoundConfigClass() {
        this.notFoundConfigClass = true;
    }

    protected ComponentConfiguration(String componentName) {
        this.componentName = componentName;
        errors = new LinkedList<FlumeConfigurationError>();
        this.type = null;
        configured = false;
    }

    public List<FlumeConfigurationError> getErrors() {
        return errors;
    }

    public void configure(Context context) throws ConfigurationException {
        failIfConfigured();
        String confType = context.getString(
                BasicConfigurationConstants.CONFIG_TYPE);
        if (confType != null && !confType.isEmpty()) {
            this.type = confType;
        }
        // Type can be set by child class constructors, so check if it was.
        if (this.type == null || this.type.isEmpty()) {
            errors.add(new FlumeConfigurationError(componentName,
                    BasicConfigurationConstants.CONFIG_TYPE,
                    FlumeConfigurationErrorType.ATTRS_MISSING, FlumeConfigurationError.ErrorOrWarning.ERROR));

            throw new ConfigurationException(
                    "Component has no type. Cannot configure. " + componentName);
        }
    }

    protected void failIfConfigured() throws ConfigurationException {
        if (configured) {
            throw new ConfigurationException("Already configured component."
                    + componentName);
        }
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return toString(0);
    }

    public String toString(int indentCount) {
        StringBuilder indentSb = new StringBuilder();

        for (int i = 0; i < indentCount; i++) {
            indentSb.append(FlumeConfiguration.INDENTSTEP);
        }

        String indent = indentSb.toString();
        StringBuilder sb = new StringBuilder(indent);

        sb.append("ComponentConfiguration[").append(componentName).append("]");
        sb.append(FlumeConfiguration.NEWLINE).append(indent).append(
                FlumeConfiguration.INDENTSTEP).append("CONFIG: ");
        sb.append(FlumeConfiguration.NEWLINE).append(indent).append(
                FlumeConfiguration.INDENTSTEP);

        return sb.toString();
    }

    public String getComponentName() {
        return componentName;
    }

    protected void setConfigured() {
        configured = true;
    }

    public enum ComponentType {
        OTHER(null),
        CONFIG_FILTER("ConfigFilter"),
        SOURCE("Source"),
        SINK("Sink"),
        SINK_PROCESSOR("SinkProcessor"),
        SINKGROUP("Sinkgroup");

        private final String componentType;

        private ComponentType(String type) {
            componentType = type;
        }

        public String getComponentType() {
            return componentType;
        }
    }
}