
package com.zhw.flume.source;

import com.zhw.flume.component.NamedComponent;
import com.zhw.flume.interceptor.Interceptor;
import com.zhw.flume.interceptor.InterceptorChain;
import com.zhw.flume.lifecycle.LifecycleAware;
import com.zhw.flume.sink.SinkRunner;

/**
 * @author zhw
 */
public interface Source extends LifecycleAware, NamedComponent {

    void setSinkRunner(SinkRunner sinkRunner);

    SinkRunner getSinkRunner();

}