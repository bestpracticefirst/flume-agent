package com.zhw.flume.source;

import com.google.common.base.Preconditions;
import com.zhw.flume.conf.Configurable;
import com.zhw.flume.interceptor.Interceptor;
import com.zhw.flume.interceptor.InterceptorBuilderFactory;
import com.zhw.flume.interceptor.InterceptorChain;
import com.zhw.flume.lifecycle.LifecycleState;
import com.zhw.flume.sink.SinkRunner;
import org.apache.commons.compress.utils.Lists;
import org.apache.flume.Context;
import org.apache.flume.FlumeException;
import org.apache.flume.channel.ChannelProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author zhw
 */
public abstract class AbstractSource implements Source, Configurable {


    private static final Logger LOG = LoggerFactory.getLogger(AbstractSource.class);

    private SinkRunner sinkRunner;

    private InterceptorChain chain;

    private LifecycleState lifecycleState;

    private String name;

    @Override
    public synchronized void start() {
        Preconditions.checkState(sinkRunner != null, "No sink runner configured");
        lifecycleState = LifecycleState.START;
    }

    @Override
    public synchronized void stop() {
        lifecycleState = LifecycleState.STOP;
    }

    @Override
    public synchronized void setSinkRunner(SinkRunner sinkRunner) {
        this.sinkRunner = sinkRunner;
    }

    @Override
    public synchronized SinkRunner getSinkRunner() {
        return sinkRunner;
    }


    @Override
    public synchronized LifecycleState getLifecycleState() {
        return lifecycleState;
    }

    @Override
    public synchronized void setName(String name) {
        this.name = name;
    }

    @Override
    public synchronized String getName() {
        return name;
    }

    @Override
    public synchronized void configure(Context context) {
        chain = new InterceptorChain();
        List<Interceptor> interceptors = Lists.newArrayList();
        String interceptorListStr = context.getString("interceptors", "");
        if (interceptorListStr.isEmpty()) {
            return;
        }
        String[] interceptorNames = interceptorListStr.split("\\s+");
        Context interceptorContexts = new Context(context.getSubProperties("interceptors."));
        // run through and instantiate all the interceptors specified in the Context
        for (String interceptorName : interceptorNames) {
            Context interceptorContext = new Context(interceptorContexts.getSubProperties(interceptorName + "."));
            String type = interceptorContext.getString("type");
            if (type == null) {
                LOG.error("Type not specified for interceptor " + interceptorName);
                throw new FlumeException("Interceptor.Type not specified for " + interceptorName);
            }
            try {
                Interceptor.Builder builder = InterceptorBuilderFactory.newInstance(type);
                builder.configure(interceptorContext);
                interceptors.add(builder.build());
            } catch (ClassNotFoundException e) {
                LOG.error("Builder class not found. Exception follows.", e);
                throw new FlumeException("Interceptor.Builder not found.", e);
            } catch (InstantiationException e) {
                LOG.error("Could not instantiate Builder. Exception follows.", e);
                throw new FlumeException("Interceptor.Builder not constructable.", e);
            } catch (IllegalAccessException e) {
                LOG.error("Unable to access Builder. Exception follows.", e);
                throw new FlumeException("Unable to access Interceptor.Builder.", e);
            }
        }

        chain.setInterceptors(interceptors);
    }

    @Override
    public String toString() {
        return this.getClass().getName() + "{name:" + name + ",state:" + lifecycleState + "}";
    }

    protected InterceptorChain getInterceptorChain(){
        return chain;
    }
}