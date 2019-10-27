
package com.zhw.flume.processor;


import com.google.common.base.Preconditions;
import com.zhw.flume.conf.ComponentConfiguration;
import com.zhw.flume.conf.ConfigurableComponent;
import com.zhw.flume.lifecycle.LifecycleState;
import com.zhw.flume.sink.Sink;
import com.zhw.flume.sink.SinkProcessor;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;

import java.util.List;

/**
 * @author zhw
 */
public class DefaultSinkProcessor implements SinkProcessor, ConfigurableComponent {

    private Sink sink;

    private LifecycleState lifecycleState;

    @Override
    public void configure(ComponentConfiguration conf) {
    }

    @Override
    public boolean process(List<Event> events) throws EventDeliveryException {
        return sink.process(events);
    }

    @Override
    public void setSinks(List<Sink> sinks) {
        Preconditions.checkNotNull(sinks);
        Preconditions.checkArgument(sinks.size() == 1, "DefaultSinkPolicy can "
                + "only handle one sink, "
                + "try using a policy that supports multiple sinks");
        sink = sinks.get(0);
    }

    @Override
    public void configure(Context context) {

    }

    @Override
    public void start() {
        Preconditions.checkNotNull(sink, "DefaultSinkProcessor sink not set");
        sink.start();
        lifecycleState = LifecycleState.START;
    }

    @Override
    public void stop() {
        Preconditions.checkNotNull(sink, "DefaultSinkProcessor sink not set");
        sink.stop();
        lifecycleState = LifecycleState.STOP;
    }

    @Override
    public LifecycleState getLifecycleState() {
        return lifecycleState;
    }
}