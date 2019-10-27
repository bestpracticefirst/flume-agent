
package com.zhw.flume.node;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.zhw.flume.conf.ComponentConfiguration;
import com.zhw.flume.conf.Configurables;
import com.zhw.flume.conf.FlumeConfiguration;
import com.zhw.flume.conf.configuration.SinkConfiguration;
import com.zhw.flume.conf.configuration.SinkGroupConfiguration;
import com.zhw.flume.conf.configuration.SourceConfiguration;
import com.zhw.flume.factory.DefaultSinkFactory;
import com.zhw.flume.factory.DefaultSourceFactory;
import com.zhw.flume.factory.SinkFactory;
import com.zhw.flume.factory.SourceFactory;
import com.zhw.flume.processor.DefaultSinkProcessor;
import com.zhw.flume.sink.Sink;
import com.zhw.flume.sink.SinkGroup;
import com.zhw.flume.sink.SinkProcessor;
import com.zhw.flume.sink.SinkRunner;
import com.zhw.flume.source.Source;
import com.zhw.flume.source.SourceRunner;
import org.apache.flume.Context;
import org.apache.flume.conf.BasicConfigurationConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author zhw
 */
public abstract class AbstractConfigurationProvider implements ConfigurationProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractConfigurationProvider.class);

    private final String agentName;

    private final SinkFactory sinkFactory;

    private final SourceFactory sourceFactory;

    public AbstractConfigurationProvider(String agentName) {
        this.agentName = agentName;
        sinkFactory = new DefaultSinkFactory();
        sourceFactory = new DefaultSourceFactory();
    }

    public String getAgentName() {
        return agentName;
    }

    protected abstract FlumeConfiguration getFlumeConfiguration();

    @Override
    public MaterializedConfiguration getConfiguration() {
        MaterializedConfiguration conf = new SimpleMaterializedConfiguration();
        FlumeConfiguration flumeConfiguration = getFlumeConfiguration();
        FlumeConfiguration.AgentConfiguration agentConf = flumeConfiguration.getConfigurationFor(getAgentName());
        if(agentConf!=null){
            Map<String, SinkRunner> sinkRunnerMap = Maps.newHashMap();
            Map<String, SourceRunner> sourceRunnerMap = Maps.newHashMap();
            try {
                loadSinks(agentConf, sinkRunnerMap);
                Preconditions.checkState(sinkRunnerMap.size() == 1, "only supply one sink runner");
                SinkRunner sinkRunner = null;
                for(SinkRunner sinkRunner1: sinkRunnerMap.values()){
                    sinkRunner = sinkRunner1;
                }
                loadSources(agentConf, sourceRunnerMap);
                for(Map.Entry<String, SourceRunner> entry : sourceRunnerMap.entrySet()) {
                    entry.getValue().getSource().setSinkRunner(sinkRunner);
                    conf.addSourceRunner(entry.getKey(), entry.getValue());
                }
                for(Map.Entry<String, SinkRunner> entry : sinkRunnerMap.entrySet()) {
                    conf.addSinkRunner(entry.getKey(), entry.getValue());
                }
            }catch (InstantiationException ex) {
                LOGGER.error("Failed to instantiate component", ex);
            } finally {
                sourceRunnerMap.clear();
                sinkRunnerMap.clear();
            }
        }
        return conf;
    }

    private void loadSources(FlumeConfiguration.AgentConfiguration agentConf,
                             Map<String, SourceRunner> sourceRunnerMap) {

        Set<String> sourceNames = agentConf.getSourceSet();
        Map<String, ComponentConfiguration> compMap =
                agentConf.getSourceConfigMap();
        /*
         * Components which have a ComponentConfiguration object
         */
        for (String sourceName : sourceNames) {
            ComponentConfiguration comp = compMap.get(sourceName);
            if(comp != null) {
                SourceConfiguration config = (SourceConfiguration) comp;

                Source source = sourceFactory.create(comp.getComponentName(),
                        comp.getType());
                try {
                    Configurables.configure(source, config);

                    sourceRunnerMap.put(comp.getComponentName(),
                            SourceRunner.forSource(source));
                } catch (Exception e) {
                    String msg = String.format("Source %s has been removed due to an " +
                            "error during configuration", sourceName);
                    LOGGER.error(msg, e);
                }
            }
        }
        /*
         * Components which DO NOT have a ComponentConfiguration object
         * and use only Context
         */
        Map<String, Context> sourceContexts = agentConf.getSourceContext();
        for (String sourceName : sourceNames) {
            Context context = sourceContexts.get(sourceName);
            if(context != null){
                Source source =
                        sourceFactory.create(sourceName,
                                context.getString(BasicConfigurationConstants.CONFIG_TYPE));
                try {
                    Configurables.configure(source, context);
                    sourceRunnerMap.put(sourceName,
                            SourceRunner.forSource(source));
                } catch (Exception e) {
                    String msg = String.format("Source %s has been removed due to an " +
                            "error during configuration", sourceName);
                    LOGGER.error(msg, e);
                }
            }
        }
    }

    protected void loadSinks(FlumeConfiguration.AgentConfiguration agentConf, Map<String, SinkRunner> sinkRunnerMap)
            throws InstantiationException {
        Set<String> sinkNames = agentConf.getSinkSet();
        Map<String, ComponentConfiguration> compMap =
                agentConf.getSinkConfigMap();
        Map<String, Sink> sinks = new HashMap<>();
        for (String sinkName : sinkNames) {
            ComponentConfiguration comp = compMap.get(sinkName);
            if(comp != null) {
                SinkConfiguration config = (SinkConfiguration) comp;
                Sink sink = sinkFactory.create(comp.getComponentName(),
                        comp.getType());
                try {
                    Configurables.configure(sink, config);
                    sinks.put(comp.getComponentName(), sink);
                } catch (Exception e) {
                    String msg = String.format("Sink %s has been removed due to an " +
                            "error during configuration", sinkName);
                    LOGGER.error(msg, e);
                }
            }
        }
        Map<String, Context> sinkContexts = agentConf.getSinkContext();
        for (String sinkName : sinkNames) {
            Context context = sinkContexts.get(sinkName);
            if(context != null) {
                Sink sink = sinkFactory.create(sinkName, context.getString(
                        BasicConfigurationConstants.CONFIG_TYPE));
                try {
                    Configurables.configure(sink, context);
                    sinks.put(sinkName, sink);
                } catch (Exception e) {
                    String msg = String.format("Sink %s has been removed due to an " +
                            "error during configuration", sinkName);
                    LOGGER.error(msg, e);
                }
            }
        }
        loadSinkGroups(agentConf, sinks, sinkRunnerMap);
    }

    protected void loadSinkGroups(FlumeConfiguration.AgentConfiguration agentConf, Map<String, Sink> sinks, Map<String, SinkRunner> sinkRunnerMap) throws InstantiationException {
        Set<String> sinkGroupNames = agentConf.getSinkgroupSet();
        Map<String, ComponentConfiguration> compMap = agentConf.getSinkGroupConfigMap();
        Map<String, String> usedSinks = new HashMap<String, String>();
        for (String groupName : sinkGroupNames) {
            ComponentConfiguration comp = compMap.get(groupName);
            if (comp != null) {
                SinkGroupConfiguration groupConf = (SinkGroupConfiguration) comp;
                List<Sink> groupSinks = new ArrayList<Sink>();
                for (String sink : groupConf.getSinks()) {
                    Sink s = sinks.remove(sink);
                    if (s == null) {
                        String sinkUser = usedSinks.get(sink);
                        if (sinkUser != null) {
                            throw new InstantiationException(
                                    String.format("Sink %s of group %s already " + "in use by group %s", sink, groupName, sinkUser));
                        } else {
                            throw new InstantiationException(String.format(
                                    "Sink %s of group %s does " + "not exist or is not properly configured", sink,
                                    groupName));
                        }
                    }
                    groupSinks.add(s);
                    usedSinks.put(sink, groupName);
                }
                try {
                    SinkGroup group = new SinkGroup(groupSinks);
                    Configurables.configure(group, groupConf);
                    sinkRunnerMap.put(comp.getComponentName(), new SinkRunner(group.getProcessor()));
                } catch (Exception e) {
                    String msg = String.format("SinkGroup %s has been removed due to " + "an error during configuration",
                            groupName);
                    LOGGER.error(msg, e);
                }
            }
        }
        // add any unassigned sinks to solo collectors
        for(Map.Entry<String, Sink> entry : sinks.entrySet()) {
            if (!usedSinks.containsValue(entry.getKey())) {
                try {
                    SinkProcessor pr = new DefaultSinkProcessor();
                    List<Sink> sinkMap = new ArrayList<Sink>();
                    sinkMap.add(entry.getValue());
                    pr.setSinks(sinkMap);
                    Configurables.configure(pr, new Context());
                    sinkRunnerMap.put(entry.getKey(),
                            new SinkRunner(pr));
                } catch(Exception e) {
                    String msg = String.format("SinkGroup %s has been removed due to " +
                            "an error during configuration", entry.getKey());
                    LOGGER.error(msg, e);
                }
            }
        }
    }
}