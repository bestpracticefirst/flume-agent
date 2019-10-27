
package com.zhw.flume.sink;

import com.zhw.flume.lifecycle.LifecycleAware;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.NamedComponent;

import java.util.List;

/**
 * @author zhw
 */
public interface Sink extends LifecycleAware, NamedComponent {

    boolean process(List<Event> events) throws EventDeliveryException;
}