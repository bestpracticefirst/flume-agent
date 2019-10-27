
package com.zhw.flume.sink;

import com.zhw.flume.conf.Configurable;
import com.zhw.flume.lifecycle.LifecycleAware;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;

import java.util.List;

/**
 * @author zhw
 */
public interface SinkProcessor extends LifecycleAware, Configurable {

    boolean process(List<Event> events) throws EventDeliveryException;

    /**
     * <p>Set all sinks to work with.</p>
     *
     * <p>Sink specific parameters are passed to the processor via configure</p>
     *
     * @param sinks A non-null, non-empty list of sinks to be chosen from by the
     * processor
     */
    void setSinks(List<Sink> sinks);
}