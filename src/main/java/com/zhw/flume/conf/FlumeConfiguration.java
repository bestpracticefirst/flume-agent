
package com.zhw.flume.conf;

import com.zhw.flume.conf.configuration.SinkConfiguration;
import com.zhw.flume.conf.configuration.SinkGroupConfiguration;
import com.zhw.flume.conf.configuration.SourceConfiguration;
import com.zhw.flume.conf.configuration.SourceType;
import org.apache.flume.Context;
import org.apache.flume.conf.BasicConfigurationConstants;
import org.apache.flume.conf.ComponentWithClassName;
import org.apache.flume.conf.ConfigFilterFactory;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.conf.FlumeConfigurationError;
import org.apache.flume.conf.FlumeConfigurationErrorType;
import org.apache.flume.conf.LogPrivacyUtil;
import org.apache.flume.conf.configfilter.ConfigFilterType;
import org.apache.flume.configfilter.ConfigFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.zhw.flume.conf.ComponentConfiguration.*;
import static org.apache.flume.conf.BasicConfigurationConstants.CONFIG_CHANNELS;
import static org.apache.flume.conf.BasicConfigurationConstants.CONFIG_CHANNELS_PREFIX;
import static org.apache.flume.conf.BasicConfigurationConstants.CONFIG_CONFIG;
import static org.apache.flume.conf.BasicConfigurationConstants.CONFIG_CONFIGFILTERS;
import static org.apache.flume.conf.BasicConfigurationConstants.CONFIG_CONFIGFILTERS_PREFIX;
import static org.apache.flume.conf.BasicConfigurationConstants.CONFIG_SINKGROUPS;
import static org.apache.flume.conf.BasicConfigurationConstants.CONFIG_SINKGROUPS_PREFIX;
import static org.apache.flume.conf.BasicConfigurationConstants.CONFIG_SINKS;
import static org.apache.flume.conf.BasicConfigurationConstants.CONFIG_SINKS_PREFIX;
import static org.apache.flume.conf.BasicConfigurationConstants.CONFIG_SOURCES;
import static org.apache.flume.conf.BasicConfigurationConstants.CONFIG_SOURCES_PREFIX;
import static org.apache.flume.conf.FlumeConfigurationError.ErrorOrWarning.WARNING;
import static org.apache.flume.conf.FlumeConfigurationErrorType.AGENT_CONFIGURATION_INVALID;
import static org.apache.flume.conf.FlumeConfigurationErrorType.AGENT_NAME_MISSING;
import static org.apache.flume.conf.FlumeConfigurationErrorType.CONFIG_ERROR;
import static org.apache.flume.conf.FlumeConfigurationErrorType.DUPLICATE_PROPERTY;
import static org.apache.flume.conf.FlumeConfigurationErrorType.INVALID_PROPERTY;
import static org.apache.flume.conf.FlumeConfigurationErrorType.PROPERTY_NAME_NULL;
import static org.apache.flume.conf.FlumeConfigurationErrorType.PROPERTY_PART_OF_ANOTHER_GROUP;
import static org.apache.flume.conf.FlumeConfigurationErrorType.PROPERTY_VALUE_NULL;
import static org.apache.flume.conf.FlumeConfigurationError.ErrorOrWarning.ERROR;

/**
 * @author zhw
 */
public class FlumeConfiguration {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlumeConfiguration.class);

    private final Map<String, AgentConfiguration> agentConfigMap;

    private final LinkedList<FlumeConfigurationError> errors;

    public static final String NEWLINE = System.getProperty("line.separator", "\n");

    public static final String INDENTSTEP = "  ";

    public FlumeConfiguration(Map<String, String> properties) {
        agentConfigMap = new HashMap<>();
        errors = new LinkedList<>();
        // Construct the in-memory component hierarchy
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (!addRawProperty(entry.getKey(), entry.getValue())) {
                LOGGER.warn("Configuration property ignored: {} = {}", entry.getKey(), entry.getValue());
            }
        }
        // Now iterate thru the agentContext and create agent configs and add them
        // to agentConfigMap

        // validate and remove improperly configured components
        validateConfiguration();
    }

    private boolean addRawProperty(String rawName, String rawValue) {
        // Null names and values not supported
        if (rawName == null || rawValue == null) {
            addError("", AGENT_NAME_MISSING, ERROR);
            return false;
        }

        // Remove leading and trailing spaces
        String name = rawName.trim();
        String value = rawValue.trim();

        // Empty values are not supported
        if (value.isEmpty()) {
            addError(name, PROPERTY_VALUE_NULL, ERROR);
            return false;
        }

        int index = name.indexOf('.');

        // All configuration keys must have a prefix defined as agent name
        if (index == -1) {
            addError(name, AGENT_NAME_MISSING, ERROR);
            return false;
        }

        String agentName = name.substring(0, index);

        // Agent name must be specified for all properties
        if (agentName.isEmpty()) {
            addError(name, AGENT_NAME_MISSING, ERROR);
            return false;
        }

        String configKey = name.substring(index + 1);

        // Configuration key must be specified for every property
        if (configKey.isEmpty()) {
            addError(name, PROPERTY_NAME_NULL, ERROR);
            return false;
        }

        AgentConfiguration aconf = agentConfigMap.get(agentName);

        if (aconf == null) {
            aconf = new AgentConfiguration(agentName, errors);
            agentConfigMap.put(agentName, aconf);
        }

        // Each configuration key must begin with one of the three prefixes:
        // sources, sinks, or channels.
        return aconf.addProperty(configKey, value);
    }

    public List<FlumeConfigurationError> getConfigurationErrors() {
        return errors;
    }

    public AgentConfiguration getConfigurationFor(String agentName) {
        return agentConfigMap.get(agentName);
    }

    private void validateConfiguration() {
        Set<Map.Entry<String, AgentConfiguration>> entries = agentConfigMap.entrySet();
        Iterator<Map.Entry<String, AgentConfiguration>> it = entries.iterator();

        while (it.hasNext()) {
            Map.Entry<String, AgentConfiguration> next = it.next();
            String agentName = next.getKey();
            AgentConfiguration aconf = next.getValue();

            if (!aconf.isValid()) {
                LOGGER.warn("Agent configuration invalid for agent '{}'. It will be removed.", agentName);
                addError(agentName, AGENT_CONFIGURATION_INVALID, ERROR);
                it.remove();
            }
            LOGGER.debug("Sinks {}\n", aconf.sinks);
            LOGGER.debug("Sources {}\n", aconf.sources);
        }

        LOGGER.info("Post-validation flume configuration contains configuration for agents: {}",
                agentConfigMap.keySet());
    }

    private void addError(String component, FlumeConfigurationErrorType errorType,
                          FlumeConfigurationError.ErrorOrWarning level) {
        errors.add(new FlumeConfigurationError(component, "", errorType, level));
    }


    public static class AgentConfiguration {

        private final String agentName;

        private String configFilters;

        private String sources;

        private String sinks;

        private String sinkgroups;

        private final Map<String, ComponentConfiguration> sourceConfigMap;

        private final Map<String, ComponentConfiguration> sinkConfigMap;

        private final Map<String, ComponentConfiguration> sinkgroupConfigMap;

        private final Map<String, ComponentConfiguration> configFilterConfigMap;

        private Map<String, Context> configFilterContextMap;

        private Map<String, Context> sourceContextMap;

        private Map<String, Context> sinkContextMap;

        private Map<String, Context> sinkGroupContextMap;

        private Set<String> sinkSet;

        private Set<String> configFilterSet;

        private Set<String> sourceSet;

        private Set<String> sinkgroupSet;

        private final List<FlumeConfigurationError> errorList;

        private List<ConfigFilter> configFiltersInstances;

        private Map<String, Pattern> configFilterPatternCache;


        private AgentConfiguration(String agentName, List<FlumeConfigurationError> errorList) {
            this.agentName = agentName;
            this.errorList = errorList;
            configFilterConfigMap = new HashMap<>();
            sourceConfigMap = new HashMap<>();
            sinkConfigMap = new HashMap<>();
            sinkgroupConfigMap = new HashMap<>();
            configFilterContextMap = new HashMap<>();
            sourceContextMap = new HashMap<>();
            sinkContextMap = new HashMap<>();
            sinkGroupContextMap = new HashMap<>();
            configFiltersInstances = new ArrayList<>();
            configFilterPatternCache = new HashMap<>();
        }


        public Map<String, ComponentConfiguration> getSourceConfigMap() {
            return sourceConfigMap;
        }

        public Map<String, ComponentConfiguration> getConfigFilterConfigMap() {
            return configFilterConfigMap;
        }

        public Map<String, ComponentConfiguration> getSinkConfigMap() {
            return sinkConfigMap;
        }

        public Map<String, ComponentConfiguration> getSinkGroupConfigMap() {
            return sinkgroupConfigMap;
        }

        public Map<String, Context> getConfigFilterContext() {
            return configFilterContextMap;
        }

        public Map<String, Context> getSourceContext() {
            return sourceContextMap;
        }

        public Map<String, Context> getSinkContext() {
            return sinkContextMap;
        }


        public Set<String> getSinkSet() {
            return sinkSet;
        }

        public Set<String> getConfigFilterSet() {
            return configFilterSet;
        }

        public Set<String> getSourceSet() {
            return sourceSet;
        }


        public Set<String> getSinkgroupSet() {
            return sinkgroupSet;
        }

        public boolean isValid() {
            configFilterSet = validateConfigFilterSet();
            createConfigFilters();
            runFiltersThroughConfigs();
            sinkSet = validateSinks();
            sinkgroupSet = validateGroups(sinkSet);
            sourceSet = validateSources();
            if (sourceSet.isEmpty() && sinkSet.isEmpty()) {
                LOGGER.warn("Agent configuration for '{}' has no sources or sinks. Will be marked invalid.", agentName);
                addError(CONFIG_SOURCES, PROPERTY_VALUE_NULL, ERROR);
                addError(CONFIG_SINKS, PROPERTY_VALUE_NULL, ERROR);
                return false;
            }
            this.configFilters = getSpaceDelimitedList(configFilterSet);
            sources = getSpaceDelimitedList(sourceSet);
            sinks = getSpaceDelimitedList(sinkSet);
            sinkgroups = getSpaceDelimitedList(sinkgroupSet);

            if (LOGGER.isDebugEnabled() && LogPrivacyUtil.allowLogPrintConfig()) {
                LOGGER.debug("Post validation configuration for {}", agentName);
                LOGGER.debug(getPostvalidationConfig());
            }

            return true;
        }

        private void runFiltersThroughConfigs() {
            runFiltersOnContextMaps(sourceContextMap, sinkContextMap, sinkGroupContextMap);
        }

        private void runFiltersOnContextMaps(Map<String, Context>... maps) {
            for (Map<String, Context> map : maps) {
                for (Context context : map.values()) {
                    for (String key : context.getParameters().keySet()) {
                        filterValue(context, key);
                    }
                }
            }
        }

        private void filterValue(Context c, String contextKey) {
            for (ConfigFilter configFilter : configFiltersInstances) {
                try {
                    Pattern pattern = configFilterPatternCache.get(configFilter.getName());
                    String currentValue = c.getString(contextKey);
                    Matcher matcher = pattern.matcher(currentValue);
                    String filteredValue = currentValue;
                    while (matcher.find()) {
                        String key = matcher.group("key");
                        LOGGER.debug("Replacing {} from config filter {}", key, configFilter.getName());
                        String filtered = configFilter.filter(key);
                        if (filtered == null) {
                            continue;
                        }
                        String fullMatch = matcher.group();
                        filteredValue = filteredValue.replace(fullMatch, filtered);
                    }
                    c.put(contextKey, filteredValue);
                } catch (Exception e) {
                    e.printStackTrace();
                    LOGGER.error("Error while matching and filtering configFilter: {} and key: {}",
                            configFilter.getName(), contextKey, e);
                }
            }
        }

        private void createConfigFilters() {
            for (String name : configFilterSet) {
                Context context = configFilterContextMap.get(name);
                ComponentConfiguration componentConfiguration = configFilterConfigMap.get(name);
                try {
                    if (context != null) {
                        ConfigFilter configFilter = ConfigFilterFactory
                                .create(name, context.getString(BasicConfigurationConstants.CONFIG_TYPE));
                        configFilter.initializeWithConfiguration(context.getParameters());
                        configFiltersInstances.add(configFilter);
                        configFilterPatternCache.put(configFilter.getName(), createConfigFilterPattern(configFilter));
                    } else if (componentConfiguration != null) {
                        ConfigFilter configFilter = ConfigFilterFactory
                                .create(componentConfiguration.getComponentName(), componentConfiguration.getType());
                        configFiltersInstances.add(configFilter);
                        configFilterPatternCache.put(configFilter.getName(), createConfigFilterPattern(configFilter));
                    }
                } catch (Exception e) {
                    LOGGER.error("Error while creating config filter {}", name, e);
                }
            }
        }

        private Pattern createConfigFilterPattern(ConfigFilter configFilter) {
            return Pattern.compile("\\$\\{" +  // ${
                    Pattern.quote(configFilter.getName()) + //<filterComponentName>
                    "\\[(|'|\")" +  // delimiter :'," or nothing
                    "(?<key>[-_a-zA-Z0-9]+)" + // key
                    "\\1\\]" + // matching delimiter
                    "\\}" // }
            );
        }

        private void addError(String key, FlumeConfigurationErrorType errorType,
                              FlumeConfigurationError.ErrorOrWarning level) {
            errorList.add(new FlumeConfigurationError(agentName, key, errorType, level));
        }

        private ConfigFilterType getKnownConfigFilter(String type) {
            return getKnownComponent(type, ConfigFilterType.values());
        }

        private <T extends ComponentWithClassName> T getKnownComponent(String type, T[] values) {
            for (T value : values) {
                if (value.toString().equalsIgnoreCase(type)) {
                    return value;
                }
                String src = value.getClassName();
                if (src != null && src.equalsIgnoreCase(type)) {
                    return value;
                }
            }
            return null;
        }

        //暂不支持ConfigFilter
        private Set<String> validateConfigFilterSet() {
            if (configFilters == null || configFilters.isEmpty()) {
                LOGGER.warn("Agent configuration for '{}' has no configfilters.", agentName);
                return new HashSet<>();
            }
            return new HashSet<>();
        }

        private Set<String> validateSinks() {
            Map<String, Context> newContextMap = new HashMap<String, Context>();
            Set<String> sinkSet;
            SinkConfiguration sinkConf = null;
            if (sinks == null || sinks.isEmpty()) {
                LOGGER.warn("Agent configuration for '{}' has no sinks.", agentName);
                addError(CONFIG_SINKS, PROPERTY_VALUE_NULL, WARNING);
                return new HashSet<String>();
            } else {
                sinkSet = new HashSet<String>(Arrays.asList(sinks.split("\\s+")));
            }
            Iterator<String> iter = sinkSet.iterator();
            while (iter.hasNext()) {
                String sinkName = iter.next();
                Context sinkContext = sinkContextMap.get(sinkName.trim());
                if (sinkContext == null) {
                    iter.remove();
                    LOGGER.warn("no context for sink{}", sinkName);
                    addError(sinkName, CONFIG_ERROR, ERROR);
                } else {
                    String config = sinkContext.getString(CONFIG_CONFIG);
                    if (config == null || config.isEmpty()) {
                        config = "OTHER";
                    }
                    try {
                        sinkConf = (SinkConfiguration) ComponentConfigurationFactory
                                .create(sinkName, config, ComponentType.SINK);
                        if (sinkConf != null) {
                            sinkConf.configure(sinkContext);
                            newContextMap.put(sinkName, sinkContext);
                            errorList.addAll(sinkConf.getErrors());
                        }

                    } catch (ConfigurationException e) {
                        iter.remove();
                        if (sinkConf != null) {
                            errorList.addAll(sinkConf.getErrors());
                        }
                        LOGGER.warn("Could not configure sink  {} due to: {}", sinkName, e.getMessage(), e);
                    }
                }

            }
            sinkContextMap = newContextMap;
            Set<String> tempSinkSet = new HashSet<>();
            tempSinkSet.addAll(sinkConfigMap.keySet());
            tempSinkSet.addAll(sinkContextMap.keySet());
            sinkSet.retainAll(tempSinkSet);
            return sinkSet;
        }

        private Set<String> validateGroups(Set<String> sinkSet) {
            Set<String> sinkgroupSet = stringToSet(sinkgroups, " ");
            Map<String, String> usedSinks = new HashMap<String, String>();
            Iterator<String> iter = sinkgroupSet.iterator();
            SinkGroupConfiguration conf;

            while (iter.hasNext()) {
                String sinkgroupName = iter.next();
                Context context = this.sinkGroupContextMap.get(sinkgroupName);
                if (context != null) {
                    try {
                        conf = (SinkGroupConfiguration) ComponentConfigurationFactory
                                .create(sinkgroupName, "sinkgroup", ComponentType.SINKGROUP);

                        conf.configure(context);
                        Set<String> groupSinks = validGroupSinks(sinkSet, usedSinks, conf);
                        errorList.addAll(conf.getErrors());
                        if (groupSinks != null && !groupSinks.isEmpty()) {
                            List<String> sinkArray = new ArrayList<String>();
                            sinkArray.addAll(groupSinks);
                            conf.setSinks(sinkArray);
                            sinkgroupConfigMap.put(sinkgroupName, conf);
                        } else {
                            addError(sinkgroupName, CONFIG_ERROR, ERROR);
                            errorList.addAll(conf.getErrors());
                            throw new ConfigurationException("No available sinks for sinkgroup: " + sinkgroupName +
                                    ". Sinkgroup will be removed");
                        }

                    } catch (ConfigurationException e) {
                        iter.remove();
                        addError(sinkgroupName, CONFIG_ERROR, ERROR);
                        LOGGER.warn("Could not configure sink group {} due to: {}", sinkgroupName, e.getMessage(), e);
                    }
                } else {
                    iter.remove();
                    addError(sinkgroupName, CONFIG_ERROR, ERROR);
                    LOGGER.warn("Configuration error for: {}.Removed.", sinkgroupName);
                }

            }

            sinkgroupSet.retainAll(sinkgroupConfigMap.keySet());
            return sinkgroupSet;
        }

        private Set<String> validateSources() {

            //Arrays.split() call will throw NPE if the sources string is empty
            if (sources == null || sources.isEmpty()) {
                LOGGER.warn("Agent configuration for '{}' has no sources.", agentName);
                addError(CONFIG_SOURCES, PROPERTY_VALUE_NULL, WARNING);
                return new HashSet<String>();
            }
            Set<String> sourceSet = new HashSet<String>(Arrays.asList(sources.split("\\s+")));
            Map<String, Context> newContextMap = new HashMap<String, Context>();
            Iterator<String> iter = sourceSet.iterator();
            SourceConfiguration srcConf = null;
            /*
             * The logic for the following code:
             *
             * Is it a known component?
             *  -Yes: Get the SourceType and set the string name of that to
             *        config and set configSpecified to true.
             *  -No.Look for config type for the given component:
             *      -Config Found:
             *        Set config to the type mentioned, set configSpecified to true
             *      -No Config found:
             *        Set config to OTHER, configSpecified to false,
             *        do basic validation. Leave the context in the
             *        contextMap to process later. Setting it to other returns
             *        a vanilla configuration(Source/Sink/Channel Configuration),
             *        which does basic syntactic validation. This object is not
             *        put into the map, so the context is retained which can be
             *        picked up - this is meant for older classes which don't
             *        implement ConfigurableComponent.
             */
            while (iter.hasNext()) {
                String sourceName = iter.next();
                Context srcContext = sourceContextMap.get(sourceName);
                String config = null;
                boolean configSpecified = false;
                if (srcContext != null) {
                    config = srcContext.getString(CONFIG_CONFIG);
                    if (config == null || config.isEmpty()) {
                        config = "OTHER";
                    }
                    try {
                        // Possible reason the configuration can fail here:
                        // Old component is configured directly using Context
                        srcConf = (SourceConfiguration) ComponentConfigurationFactory
                                .create(sourceName, config, ComponentType.SOURCE);
                        if (srcConf != null) {
                            srcConf.configure(srcContext);
                            newContextMap.put(sourceName, srcContext);
                            errorList.addAll(srcConf.getErrors());
                        }

                    } catch (ConfigurationException e) {
                        if (srcConf != null) {
                            errorList.addAll(srcConf.getErrors());
                        }
                        iter.remove();
                        LOGGER.warn("Could not configure source  {} due to: {}", sourceName, e.getMessage(), e);
                    }
                } else {
                    iter.remove();
                    addError(sourceName, CONFIG_ERROR, ERROR);
                    LOGGER.warn("Configuration empty for: {}.Removed.", sourceName);
                }
            }

            // validateComponent(sourceSet, sourceConfigMap, CLASS_SOURCE, ATTR_TYPE,
            // ATTR_CHANNELS);
            sourceContextMap = newContextMap;
            Set<String> tempsourceSet = new HashSet<String>();
            tempsourceSet.addAll(sourceContextMap.keySet());
            tempsourceSet.addAll(sourceConfigMap.keySet());
            sourceSet.retainAll(tempsourceSet);
            return sourceSet;
        }

        private Set<String> validGroupSinks(Set<String> sinkSet, Map<String, String> usedSinks,
                                            SinkGroupConfiguration groupConf) {
            Set<String> groupSinks = Collections.synchronizedSet(new HashSet<String>(groupConf.getSinks()));

            if (groupSinks.isEmpty()) {
                return null;
            }
            Iterator<String> sinkIt = groupSinks.iterator();
            while (sinkIt.hasNext()) {
                String curSink = sinkIt.next();
                if (usedSinks.containsKey(curSink)) {
                    LOGGER.warn("Agent configuration for '{}' sinkgroup '{}' sink '{}' in use by another group: " +
                                    "'{}', sink not added", agentName, groupConf.getComponentName(), curSink,
                            usedSinks.get(curSink));
                    addError(groupConf.getComponentName(), PROPERTY_PART_OF_ANOTHER_GROUP, ERROR);
                    sinkIt.remove();
                } else if (!sinkSet.contains(curSink)) {
                    LOGGER.warn(
                            "Agent configuration for '{}' sinkgroup '{}' sink not found: '{}', " + " sink not added",
                            agentName, groupConf.getComponentName(), curSink);
                    addError(curSink, INVALID_PROPERTY, ERROR);
                    sinkIt.remove();
                } else {
                    usedSinks.put(curSink, groupConf.getComponentName());
                }
            }
            return groupSinks;
        }

        private String getSpaceDelimitedList(Set<String> entries) {
            if (entries.isEmpty()) {
                return null;
            }

            StringBuilder sb = new StringBuilder();

            for (String entry : entries) {
                sb.append(" ").append(entry);
            }

            return sb.toString().trim();
        }

        private static Set<String> stringToSet(String target, String delim) {
            Set<String> out = new HashSet<String>();
            if (target == null || target.trim().length() == 0) {
                return out;
            }
            StringTokenizer t = new StringTokenizer(target, delim);
            while (t.hasMoreTokens()) {
                out.add(t.nextToken());
            }
            return out;
        }

        private String getPostvalidationConfig() {
            StringBuilder sb = new StringBuilder("AgentConfiguration created without Configuration stubs " +
                    "for which only basic syntactical validation was performed[");
            sb.append(agentName).append("]").append(NEWLINE);
            if (!sourceContextMap.isEmpty() || !sinkContextMap.isEmpty()) {
                if (!sourceContextMap.isEmpty()) {
                    sb.append("SOURCES: ").append(sourceContextMap).append(NEWLINE);
                }

                if (!sinkContextMap.isEmpty()) {
                    sb.append("SINKS: ").append(sinkContextMap).append(NEWLINE);
                }
            }

            if (!sourceConfigMap.isEmpty() || !sinkConfigMap.isEmpty()) {
                sb.append("AgentConfiguration created with Configuration stubs " +
                        "for which full validation was performed[");
                sb.append(agentName).append("]").append(NEWLINE);

                if (!sourceConfigMap.isEmpty()) {
                    sb.append("SOURCES: ").append(sourceConfigMap).append(NEWLINE);
                }

                if (!sinkConfigMap.isEmpty()) {
                    sb.append("SINKS: ").append(sinkConfigMap).append(NEWLINE);
                }
            }

            return sb.toString();
        }

        private boolean addProperty(String key, String value) {
            // Check for configFilters
            if (CONFIG_CONFIGFILTERS.equals(key)) {
                if (configFilters == null) {
                    configFilters = value;
                    return true;
                } else {
                    LOGGER.warn("Duplicate configfilter list specified for agent: {}", agentName);
                    addError(CONFIG_CONFIGFILTERS, DUPLICATE_PROPERTY, ERROR);
                    return false;
                }
            }
            // Check for sources
            if (CONFIG_SOURCES.equals(key)) {
                if (sources == null) {
                    sources = value;
                    return true;
                } else {
                    LOGGER.warn("Duplicate source list specified for agent: {}", agentName);
                    addError(CONFIG_SOURCES, DUPLICATE_PROPERTY, ERROR);
                    return false;
                }
            }

            // Check for sinks
            if (CONFIG_SINKS.equals(key)) {
                if (sinks == null) {
                    sinks = value;
                    LOGGER.info("Added sinks: {} Agent: {}", sinks, agentName);
                    return true;
                } else {
                    LOGGER.warn("Duplicate sink list specfied for agent: {}", agentName);
                    addError(CONFIG_SINKS, DUPLICATE_PROPERTY, ERROR);
                    return false;
                }
            }


            // Check for sinkgroups
            if (CONFIG_SINKGROUPS.equals(key)) {
                if (sinkgroups == null) {
                    sinkgroups = value;

                    return true;
                } else {
                    LOGGER.warn("Duplicate sinkgroup list specfied for agent: {}", agentName);
                    addError(CONFIG_SINKGROUPS, DUPLICATE_PROPERTY, ERROR);
                    return false;
                }
            }

            if (addAsSourceConfig(key, value) || addAsSinkConfig(key, value) || addAsSinkGroupConfig(key, value) ||
                    addAsConfigFilterConfig(key, value)) {
                return true;
            }

            LOGGER.warn("Invalid property specified: {}", key);
            addError(key, INVALID_PROPERTY, ERROR);
            return false;
        }

        private boolean addAsConfigFilterConfig(String key, String value) {
            return addComponentConfig(key, value, CONFIG_CONFIGFILTERS_PREFIX, configFilterContextMap);
        }

        private boolean addAsSinkGroupConfig(String key, String value) {
            return addComponentConfig(key, value, CONFIG_SINKGROUPS_PREFIX, sinkGroupContextMap);
        }

        private boolean addAsSinkConfig(String key, String value) {
            return addComponentConfig(key, value, CONFIG_SINKS_PREFIX, sinkContextMap);
        }

        private boolean addAsSourceConfig(String key, String value) {
            return addComponentConfig(key, value, CONFIG_SOURCES_PREFIX, sourceContextMap);
        }

        private ComponentNameAndConfigKey parseConfigKey(String key, String prefix) {
            // key must start with prefix
            if (!key.startsWith(prefix)) {
                return null;
            }

            // key must have a component name part after the prefix of the format:
            // <prefix><component-name>.<config-key>
            int index = key.indexOf('.', prefix.length() + 1);

            if (index == -1) {
                return null;
            }

            String name = key.substring(prefix.length(), index);
            String configKey = key.substring(prefix.length() + name.length() + 1);

            // name and config key must be non-empty
            if (name.isEmpty() || configKey.isEmpty()) {
                return null;
            }

            return new ComponentNameAndConfigKey(name, configKey);
        }


        private boolean addComponentConfig(String key, String value, String configPrefix,
                                           Map<String, Context> contextMap

        ) {
            FlumeConfiguration.ComponentNameAndConfigKey parsed = parseConfigKey(key, configPrefix);
            if (parsed != null) {
                String name = parsed.getComponentName().trim();
                LOGGER.info("Processing:{}", name);
                Context context = contextMap.get(name);

                if (context == null) {
                    LOGGER.debug("Created context for {}: {}", name, parsed.getConfigKey());
                    context = new Context();
                    contextMap.put(name, context);
                }

                context.put(parsed.getConfigKey(), value);
                return true;
            }

            return false;
        }

    }


    public static class ComponentNameAndConfigKey {

        private final String componentName;

        private final String configKey;

        private ComponentNameAndConfigKey(String name, String configKey) {
            this.componentName = name;
            this.configKey = configKey;
        }

        public String getComponentName() {
            return componentName;
        }

        public String getConfigKey() {
            return configKey;
        }
    }
}