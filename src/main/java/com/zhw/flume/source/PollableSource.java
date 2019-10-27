
package com.zhw.flume.source;

import org.apache.flume.EventDeliveryException;

/**
 * @author zhw
 */
public interface PollableSource extends Source {

    Status process() throws EventDeliveryException;

    long getBackOffSleepIncrement();

    long getMaxBackOffSleepInterval();

    static enum Status {
        READY, BACKOFF
    }
}