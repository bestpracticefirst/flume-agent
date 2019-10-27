package com.zhw.flume.threadpool;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * 单调度线程，用于执行调度的任务
 *
 * @author zhw
 */
public class SingleScheduledThreadPool {

    private static final Logger LOG = LoggerFactory.getLogger(SingleScheduledThreadPool.class);

    private static ScheduledExecutorService scheduledExecutorService;

    private SingleScheduledThreadPool() {
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("singleScheduled").build();
        scheduledExecutorService = new ScheduledThreadPoolExecutor(1, threadFactory);
    }

    public static SingleScheduledThreadPool getInstance() {
        return Singleton.INSTANCE.getSingleScheduledThreadPool();
    }

    public void scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        scheduledExecutorService.scheduleAtFixedRate(command, initialDelay, period, unit);
    }

    public void scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit){
        scheduledExecutorService.scheduleWithFixedDelay(command, initialDelay, delay, unit);
    }

    public synchronized void stop() {
        if (scheduledExecutorService.isShutdown()) {
            return;
        }
        scheduledExecutorService.shutdown();
        try {
            if (!scheduledExecutorService.awaitTermination(1, TimeUnit.SECONDS)) {
                scheduledExecutorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            LOG.error("SingleScheduledThreadPool stop failed, ", e);
        }
    }

    private enum Singleton {
        INSTANCE;

        private SingleScheduledThreadPool singleScheduledThreadPool;

        Singleton() {
            singleScheduledThreadPool = new SingleScheduledThreadPool();
        }

        public SingleScheduledThreadPool getSingleScheduledThreadPool() {
            return singleScheduledThreadPool;
        }
    }
}