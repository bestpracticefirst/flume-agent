
package com.zhw.flume.sink;

import com.zhw.flume.lifecycle.LifecycleState;

import java.util.List;

/**
 * @author zhw
 */
public abstract class AbstractSink implements Sink {

    private String name;

    private LifecycleState lifecycleState;

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void start() {
        lifecycleState = LifecycleState.START;
    }

    @Override
    public void stop() {
        lifecycleState = LifecycleState.STOP;
    }

    @Override
    public LifecycleState getLifecycleState() {
        return lifecycleState;
    }
}