
package com.zhw.flume.interceptor;

import org.apache.flume.Event;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhw
 */
public class InterceptorChain implements Interceptor {

    private List<Interceptor> interceptors;


    public InterceptorChain() {
        interceptors = new ArrayList<>();
    }

    public void setInterceptors(List<Interceptor> interceptors) {
        this.interceptors = interceptors;
    }

    @Override
    public void initialize() {
        for (Interceptor interceptor : interceptors) {
            interceptor.initialize();
        }
    }

    @Override
    public Event intercept(Event event) {
        if (event == null) {
            return null;
        }
        for (Interceptor interceptor : interceptors) {
            event = interceptor.intercept(event);
        }
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        if (events.isEmpty()) {
            return events;
        }
        for (Interceptor interceptor : interceptors) {
            events = interceptor.intercept(events);
        }
        return events;
    }

    @Override
    public void close() {
        for (Interceptor interceptor: interceptors){
            interceptor.close();
        }
    }
}