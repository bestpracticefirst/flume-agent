
package com.zhw.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.source.shaded.guava.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author zhw
 */
public class QpsInterceptor implements Interceptor {

    private static final Logger LOG = LoggerFactory.getLogger(QpsInterceptor.class);

    private long rate;
    private RateLimiter rateLimiter;

    private QpsInterceptor(long rate) {
        this.rate = rate;
    }

    @Override
    public void initialize() {
        rateLimiter = RateLimiter.create(rate);
    }

    @Override
    public Event intercept(Event event) {
        rateLimiter.acquire(1);
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        for (Event event : events) {
            intercept(event);
        }
        return events;
    }

    @Override
    public void close() {
    }

    public static class Builder implements Interceptor.Builder {

        private long rate;

        @Override
        public Interceptor build() {
            return new QpsInterceptor(rate);
        }

        @Override
        public void configure(Context context) {
            rate = context.getLong(Constants.RATE, Constants.DEFAULT_RATE);
        }

        static class Constants {

            static long DEFAULT_RATE = 200L;
            static String RATE = "rate";
        }
    }
}