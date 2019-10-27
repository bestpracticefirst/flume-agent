
package com.zhw.flume.interceptor;

import org.apache.flume.Event;
import org.apache.flume.conf.Configurable;

import java.util.List;

/**
 * @author zhw
 */
public interface Interceptor {

    void initialize();

    Event intercept(Event event);


    List<Event> intercept(List<Event> events);


    void close();

    public interface Builder extends Configurable {
        Interceptor build();
    }
}