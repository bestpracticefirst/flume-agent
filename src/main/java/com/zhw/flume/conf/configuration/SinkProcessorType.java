
package com.zhw.flume.conf.configuration;

import com.zhw.flume.conf.ComponentWithClassName;

/**
 * @author zhw
 */
public enum SinkProcessorType implements ComponentWithClassName {

    OTHER(null), DEFAULT("com.zhw.flume.sink.DefaultSinkProcessor");

    private final String processorClassName;

    SinkProcessorType(String processorClassName) {
        this.processorClassName = processorClassName;
    }

    @Override
    public String getClassName() {
        return processorClassName;
    }
}