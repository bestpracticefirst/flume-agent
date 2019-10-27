
package com.zhw.flume.node;

import com.google.common.collect.ImmutableMap;
import com.zhw.flume.sink.SinkRunner;
import com.zhw.flume.source.SourceRunner;
import org.apache.flume.Channel;

import java.util.HashMap;
import java.util.Map;

/**
 * @author zhw
 */
public class SimpleMaterializedConfiguration implements  MaterializedConfiguration{

    private final Map<String, SourceRunner> sourceRunners;
    private final Map<String, SinkRunner> sinkRunners;
    public SimpleMaterializedConfiguration() {
        sourceRunners = new HashMap<>();
        sinkRunners = new HashMap<>();
    }

    @Override
    public String toString() {
        return "{ sourceRunners:" + sourceRunners + " sinkRunners:" + sinkRunners+" }";
    }
    @Override
    public void addSourceRunner(String name, SourceRunner sourceRunner) {
        sourceRunners.put(name, sourceRunner);
    }

    @Override
    public void addSinkRunner(String name, SinkRunner sinkRunner) {
        sinkRunners.put(name, sinkRunner);
    }

    @Override
    public ImmutableMap<String, SourceRunner> getSourceRunners() {
        return ImmutableMap.copyOf(sourceRunners);
    }

    @Override
    public ImmutableMap<String, SinkRunner> getSinkRunners() {
        return ImmutableMap.copyOf(sinkRunners);
    }
}