package com.zhw.flume.lifecycle;

/**
 * @author zhw
 */
public interface LifecycleAware {

    //启动组件
    void start();

    //停止组件
    void stop();

    //获取状态信息
    LifecycleState getLifecycleState();
}