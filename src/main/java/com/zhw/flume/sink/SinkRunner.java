
package com.zhw.flume.sink;

import com.zhw.flume.lifecycle.LifecycleAware;
import com.zhw.flume.lifecycle.LifecycleState;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;

import java.util.List;

/**
 * @author zhw
 */
public class SinkRunner implements LifecycleAware {

    private SinkProcessor policy;

    private LifecycleState lifecycleState;

    public SinkRunner() {
        lifecycleState = LifecycleState.IDLE;
    }

    public SinkRunner(SinkProcessor policy) {
        this();
        this.policy = policy;
    }

    @Override
    public void start() {
        policy.start();
        lifecycleState = LifecycleState.START;
    }

    @Override
    public void stop() {
        policy.stop();
        lifecycleState = LifecycleState.STOP;
    }

    @Override
    public LifecycleState getLifecycleState() {
        return lifecycleState;
    }

    public boolean batchProcess(List<Event> events) throws EventDeliveryException {
        if(events.isEmpty()){
            return true;
        }
        return policy.process(events);
    }
}