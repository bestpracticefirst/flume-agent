
package com.zhw.flume.source;

import com.zhw.flume.lifecycle.LifecycleState;
import org.apache.flume.CounterGroup;
import org.apache.flume.EventDeliveryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author zhw
 */
public class PollableSourceRunner extends SourceRunner {

    private static final Logger logger = LoggerFactory.getLogger(org.apache.flume.source.PollableSourceRunner.class);

    private AtomicBoolean shouldStop;

    private CounterGroup counterGroup;
    private PollingRunner runner;
    private Thread runnerThread;
    private LifecycleState lifecycleState;

    public PollableSourceRunner() {
        shouldStop = new AtomicBoolean();
        counterGroup = new CounterGroup();
        lifecycleState = LifecycleState.IDLE;
    }

    @Override
    public void start() {
        PollableSource source = (PollableSource) getSource();
        source.start();

        runner = new PollingRunner();

        runner.source = source;
        runner.counterGroup = counterGroup;
        runner.shouldStop = shouldStop;

        runnerThread = new Thread(runner);
        runnerThread.setName(getClass().getSimpleName() + "-" +
                source.getClass().getSimpleName() + "-" + source.getName());
        runnerThread.start();

        lifecycleState = LifecycleState.START;
    }

    @Override
    public void stop() {

        runner.shouldStop.set(true);

        try {
            runnerThread.interrupt();
            runnerThread.join();
        } catch (InterruptedException e) {
            logger.warn("Interrupted while waiting for polling runner to stop. Please report this.", e);
            Thread.currentThread().interrupt();
        }

        Source source = getSource();
        source.stop();

        lifecycleState = LifecycleState.STOP;
    }

    @Override
    public String toString() {
        return "PollableSourceRunner: { source:" + getSource() + " counterGroup:"
                + counterGroup + " }";
    }

    @Override
    public LifecycleState getLifecycleState() {
        return lifecycleState;
    }

    public static class PollingRunner implements Runnable {

        private PollableSource source;
        private AtomicBoolean shouldStop;
        private CounterGroup counterGroup;

        @Override
        public void run() {
            logger.debug("Polling runner starting. Source:{}", source);

            while (!shouldStop.get()) {
                counterGroup.incrementAndGet("runner.polls");

                try {
                    if (source.process().equals(PollableSource.Status.BACKOFF)) {
                        counterGroup.incrementAndGet("runner.backoffs");

                        Thread.sleep(Math.min(
                                counterGroup.incrementAndGet("runner.backoffs.consecutive")
                                        * source.getBackOffSleepIncrement(), source.getMaxBackOffSleepInterval()));
                    } else {
                        counterGroup.set("runner.backoffs.consecutive", 0L);
                    }
                } catch (InterruptedException e) {
                    logger.info("Source runner interrupted. Exiting");
                    counterGroup.incrementAndGet("runner.interruptions");
                } catch (EventDeliveryException e) {
                    logger.error("Unable to deliver event. Exception follows.", e);
                    counterGroup.incrementAndGet("runner.deliveryErrors");
                } catch (Exception e) {
                    counterGroup.incrementAndGet("runner.errors");
                    logger.error("Unhandled exception, logging and sleeping for " +
                            source.getMaxBackOffSleepInterval() + "ms", e);
                    try {
                        Thread.sleep(source.getMaxBackOffSleepInterval());
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    }
                }
            }

            logger.debug("Polling runner exiting. Metrics:{}", counterGroup);
        }

    }
}