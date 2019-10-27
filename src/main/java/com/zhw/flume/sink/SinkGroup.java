
package com.zhw.flume.sink;

import com.zhw.flume.conf.ComponentConfiguration;
import com.zhw.flume.conf.Configurable;
import com.zhw.flume.conf.ConfigurableComponent;
import com.zhw.flume.conf.configuration.SinkGroupConfiguration;
import com.zhw.flume.processor.SinkProcessorFactory;
import org.apache.flume.Context;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.ConfigurationException;

import java.util.List;

/**
 * @author zhw
 */
public class SinkGroup implements Configurable, ConfigurableComponent {

    List<Sink> sinks;
    SinkProcessor processor;
    SinkGroupConfiguration conf;

    public SinkGroup(List<Sink> groupSinks) {
        sinks = groupSinks;
    }

    @Override
    public void configure(Context context) {
        conf = new SinkGroupConfiguration("sinkgrp");
        try {
            conf.configure(context);
        } catch (ConfigurationException e) {
            throw new FlumeException("Invalid Configuration!", e);
        }
        configure(conf);

    }

    public SinkProcessor getProcessor() {
        return processor;
    }

    @Override
    public void configure(ComponentConfiguration conf) {
        this.conf = (SinkGroupConfiguration) conf;
        processor =
                SinkProcessorFactory.getProcessor(this.conf.getProcessorContext(),
                        sinks);
    }
}