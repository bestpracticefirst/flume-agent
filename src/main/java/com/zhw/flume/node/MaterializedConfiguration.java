
package com.zhw.flume.node;


import com.google.common.collect.ImmutableMap;
import com.zhw.flume.sink.SinkRunner;
import com.zhw.flume.source.SourceRunner;

/**
 * @author zhw
 */
public interface MaterializedConfiguration {

    void addSourceRunner(String name, SourceRunner sourceRunner);

    void addSinkRunner(String name, SinkRunner sinkRunner);

    ImmutableMap<String, SourceRunner> getSourceRunners();

    ImmutableMap<String, SinkRunner> getSinkRunners();
}