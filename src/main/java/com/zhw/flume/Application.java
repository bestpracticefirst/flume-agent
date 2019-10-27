package com.zhw.flume;

import com.google.common.collect.Lists;
import com.google.common.eventbus.Subscribe;
import com.zhw.flume.lifecycle.LifecycleAware;
import com.zhw.flume.lifecycle.LifecycleState;
import com.zhw.flume.lifecycle.LifecycleSupervisor;
import com.zhw.flume.node.MaterializedConfiguration;
import com.zhw.flume.node.PropertiesFileConfigurationProvider;
import com.zhw.flume.sink.SinkRunner;
import com.zhw.flume.source.SourceRunner;
import org.apache.commons.cli.HelpFormatter;
import org.apache.flume.Context;
import org.apache.flume.instrumentation.MonitorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.flume.instrumentation.MonitoringType;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;

/**
 * Hello world!
 */
public class Application {

    private static final Logger logger = LoggerFactory.getLogger(Application.class);

    public static final String CONF_MONITOR_CLASS = "flume.monitoring.type";

    public static final String CONF_MONITOR_PREFIX = "flume.monitoring.";

    private final List<LifecycleAware> components;

    private final LifecycleSupervisor supervisor;

    private MaterializedConfiguration materializedConfiguration;

    private MonitorService monitorServer;

    private final ReentrantLock lifecycleLock = new ReentrantLock();

    public Application() {
        this(new ArrayList<LifecycleAware>(0));
    }

    public Application(List<LifecycleAware> components) {
        this.components = components;
        supervisor = new LifecycleSupervisor();
    }

    public void start() {
        lifecycleLock.lock();
        try {
            for (LifecycleAware component : components) {
                supervisor.supervise(component, new LifecycleSupervisor.SupervisorPolicy.AlwaysRestartPolicy(),
                        LifecycleState.START);
            }
        } finally {
            lifecycleLock.unlock();
        }
    }

    @Subscribe
    public void handleConfigurationEvent(MaterializedConfiguration conf) {
        try {
            lifecycleLock.lockInterruptibly();
            stopAllComponents();
            startAllComponents(conf);
        } catch (InterruptedException e) {
            logger.info("Interrupted while trying to handle configuration event");
            return;
        } finally {
            // If interrupted while trying to lock, we don't own the lock, so must not attempt to unlock
            if (lifecycleLock.isHeldByCurrentThread()) {
                lifecycleLock.unlock();
            }
        }
    }

    public void stop() {
        lifecycleLock.lock();
        stopAllComponents();
        try {
            supervisor.stop();
            if (monitorServer != null) {
                monitorServer.stop();
            }
        } finally {
            lifecycleLock.unlock();
        }
    }

    private void stopAllComponents() {
        if (this.materializedConfiguration != null) {

            for (Map.Entry<String, SourceRunner> entry : this.materializedConfiguration.getSourceRunners().entrySet()) {
                try {
                    logger.info("Stopping Source " + entry.getKey());
                    supervisor.unsupervise(entry.getValue());
                } catch (Exception e) {
                    logger.error("Error while stopping {}", entry.getValue(), e);
                }
            }
            logger.info("Shutting down configuration: {}", this.materializedConfiguration);
            for (Map.Entry<String, SinkRunner> entry : this.materializedConfiguration.getSinkRunners().entrySet()) {
                try {
                    logger.info("Stopping Sink " + entry.getKey());
                    supervisor.unsupervise(entry.getValue());
                } catch (Exception e) {
                    logger.error("Error while stopping {}", entry.getValue(), e);
                }
            }
        }
        if (monitorServer != null) {
            monitorServer.stop();
        }
    }

    private void startAllComponents(MaterializedConfiguration materializedConfiguration) {
        logger.info("Starting new configuration:{}", materializedConfiguration);

        this.materializedConfiguration = materializedConfiguration;

        /*
         * Wait for all channels to start.
         */

        for (Map.Entry<String, SinkRunner> entry : materializedConfiguration.getSinkRunners().entrySet()) {
            try {
                logger.info("Starting Sink " + entry.getKey());
                supervisor.supervise(entry.getValue(), new LifecycleSupervisor.SupervisorPolicy.AlwaysRestartPolicy(),
                        LifecycleState.START);
            } catch (Exception e) {
                logger.error("Error while starting {}", entry.getValue(), e);
            }
        }

        for (Map.Entry<String, SourceRunner> entry : materializedConfiguration.getSourceRunners().entrySet()) {
            try {
                logger.info("Starting Source " + entry.getKey());
                supervisor.supervise(entry.getValue(), new LifecycleSupervisor.SupervisorPolicy.AlwaysRestartPolicy(),
                        LifecycleState.START);
            } catch (Exception e) {
                logger.error("Error while starting {}", entry.getValue(), e);
            }
        }

        this.loadMonitoring();
    }

    @SuppressWarnings("unchecked")
    private void loadMonitoring() {
        Properties systemProps = System.getProperties();
        Set<String> keys = systemProps.stringPropertyNames();
        try {
            if (keys.contains(CONF_MONITOR_CLASS)) {
                String monitorType = systemProps.getProperty(CONF_MONITOR_CLASS);
                Class<? extends MonitorService> klass;
                try {
                    //Is it a known type?
                    klass = MonitoringType.valueOf(monitorType.toUpperCase(Locale.ENGLISH)).getMonitorClass();
                } catch (Exception e) {
                    //Not a known type, use FQCN
                    klass = (Class<? extends MonitorService>) Class.forName(monitorType);
                }
                this.monitorServer = klass.newInstance();
                Context context = new Context();
                for (String key : keys) {
                    if (key.startsWith(CONF_MONITOR_PREFIX)) {
                        context.put(key.substring(CONF_MONITOR_PREFIX.length()), systemProps.getProperty(key));
                    }
                }
                monitorServer.configure(context);
                monitorServer.start();
            }
        } catch (Exception e) {
            logger.warn("Error starting monitoring. " + "Monitoring might not be available.", e);
        }

    }

    public static void main(String[] args) {

        try {

            Options options = new Options();


            Option option = new Option("f", "conf-file", true, "specify a conf file");
            option.setRequired(true);
            options.addOption(option);
            options.addOption(option);

            option = new Option("h", "help", false, "display help text");
            options.addOption(option);

            CommandLineParser parser = new GnuParser();
            CommandLine commandLine = parser.parse(options, args);

            ClassLoader classLoader = Application.class.getClassLoader();
            String properties = commandLine.getOptionValue('f');
            InputStream is = classLoader.getResourceAsStream(properties);
            String agentName = "agent";
            if (commandLine.hasOption('h')) {
                new HelpFormatter().printHelp("flume-ng agent", options, true);
                return;
            }
            /*
             * The following is to ensure that by default the agent
             * will fail on startup if the file does not exist.
             */
            if (is == null) {
                // If command line invocation, then need to fail fast
                logger.error("{} not exist", properties);
            }
            List<LifecycleAware> components = Lists.newArrayList();
            Application application;
            PropertiesFileConfigurationProvider configurationProvider =
                    new PropertiesFileConfigurationProvider(agentName, is);
            application = new Application();
            application.handleConfigurationEvent(configurationProvider.getConfiguration());
            application.start();

            final Application appReference = application;
            Runtime.getRuntime().addShutdownHook(new Thread("agent-shutdown-hook") {

                @Override
                public void run() {
                    appReference.stop();
                }
            });

        } catch (Exception e) {
            logger.error("A fatal error occurred while running. Exception follows.", e);
        }
    }
}
