package com.zhw.flume.source.tail;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.zhw.flume.conf.BatchSizeSupported;
import com.zhw.flume.conf.Configurable;
import com.zhw.flume.source.AbstractSource;
import com.zhw.flume.source.PollableSource;
import com.zhw.flume.threadpool.SingleScheduledThreadPool;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.source.PollableSourceConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Table;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static com.zhw.flume.source.PollableSource.Status.BACKOFF;
import static com.zhw.flume.source.tail.TailDirSourceConfigurationConstants.BATCH_SIZE;
import static com.zhw.flume.source.tail.TailDirSourceConfigurationConstants.BYTE_OFFSET_HEADER;
import static com.zhw.flume.source.tail.TailDirSourceConfigurationConstants.CACHE_PATTERN_MATCHING;
import static com.zhw.flume.source.tail.TailDirSourceConfigurationConstants.DEFAULT_BATCH_SIZE;
import static com.zhw.flume.source.tail.TailDirSourceConfigurationConstants.DEFAULT_BYTE_OFFSET_HEADER;
import static com.zhw.flume.source.tail.TailDirSourceConfigurationConstants.DEFAULT_CACHE_PATTERN_MATCHING;
import static com.zhw.flume.source.tail.TailDirSourceConfigurationConstants.DEFAULT_FILENAME_HEADER_KEY;
import static com.zhw.flume.source.tail.TailDirSourceConfigurationConstants.DEFAULT_FILE_HEADER;
import static com.zhw.flume.source.tail.TailDirSourceConfigurationConstants.DEFAULT_IDLE_TIMEOUT;
import static com.zhw.flume.source.tail.TailDirSourceConfigurationConstants.DEFAULT_MAX_BATCH_COUNT;
import static com.zhw.flume.source.tail.TailDirSourceConfigurationConstants.DEFAULT_POSITION_FILE;
import static com.zhw.flume.source.tail.TailDirSourceConfigurationConstants.DEFAULT_SKIP_TO_END;
import static com.zhw.flume.source.tail.TailDirSourceConfigurationConstants.DEFAULT_WRITE_POS_INTERVAL;
import static com.zhw.flume.source.tail.TailDirSourceConfigurationConstants.FILENAME_HEADER;
import static com.zhw.flume.source.tail.TailDirSourceConfigurationConstants.FILENAME_HEADER_KEY;
import static com.zhw.flume.source.tail.TailDirSourceConfigurationConstants.FILE_GROUPS;
import static com.zhw.flume.source.tail.TailDirSourceConfigurationConstants.FILE_GROUPS_PREFIX;
import static com.zhw.flume.source.tail.TailDirSourceConfigurationConstants.HEADERS_PREFIX;
import static com.zhw.flume.source.tail.TailDirSourceConfigurationConstants.IDLE_TIMEOUT;
import static com.zhw.flume.source.tail.TailDirSourceConfigurationConstants.MAX_BATCH_COUNT;
import static com.zhw.flume.source.tail.TailDirSourceConfigurationConstants.POSITION_FILE;
import static com.zhw.flume.source.tail.TailDirSourceConfigurationConstants.SKIP_TO_END;
import static com.zhw.flume.source.tail.TailDirSourceConfigurationConstants.WRITE_POS_INTERVAL;

/**
 * @author zhw
 */
public class TailDirSource extends AbstractSource implements BatchSizeSupported, PollableSource {

    private static final Logger LOG = LoggerFactory.getLogger(TailDirSource.class);

    private Map<String, String> filePaths;

    private Table<String, String, String> headerTable;

    private int eventProcessMin = 500;

    private int batchSize;

    private String positionFilePath;

    private boolean skipToEnd;

    private boolean byteOffsetHeader;

    private ReliableTailDirEventReader reader;

    private SingleScheduledThreadPool checker;

    private int retryInterval = 1000;

    private int maxRetryInterval = 1000;

    private int idleTimeout;

    private int checkIdleInterval = 5000;

    private int writePosInitDelay = 5000;

    private int writePosInterval;

    private boolean cachePatternMatching;

    private List<Long> existingInodes = new CopyOnWriteArrayList<>();

    private List<Long> idleInodes = new CopyOnWriteArrayList<Long>();

    private Long backoffSleepIncrement;

    private Long maxBackOffSleepInterval;

    private boolean fileHeader;

    private String fileHeaderKey;

    private Long maxBatchCount;

    @Override
    public synchronized void start() {
        checker = SingleScheduledThreadPool.getInstance();
        LOG.info("{} TaildirSource source starting with directory: {}", getName(), filePaths);
        try {
            reader = new ReliableTailDirEventReader.Builder().filePaths(filePaths).headerTable(headerTable)
                    .positionFilePath(positionFilePath).skipToEnd(skipToEnd).addByteOffset(byteOffsetHeader)
                    .cachePatternMatching(cachePatternMatching).annotateFileName(fileHeader)
                    .fileNameHeader(fileHeaderKey).build();
        } catch (IOException e) {
            LOG.error("Error instantiating ReliableTaildirEventReader, ", e);
            throw new FlumeException("Error instantiating ReliableTaildirEventReader", e);
        }
        checker.scheduleWithFixedDelay(new idleFileCheckerRunnable(), idleTimeout, checkIdleInterval,
                TimeUnit.MILLISECONDS);
        checker.scheduleWithFixedDelay(new PositionWriterRunnable(), writePosInitDelay, writePosInterval,
                TimeUnit.MILLISECONDS);
        getSinkRunner().start();
        super.start();
    }

    @Override
    public synchronized void stop() {
        try {
            super.stop();
            getSinkRunner().stop();
            checker.stop();
            writePosition();
            reader.close();
        } catch (IOException e) {
            LOG.info("Failed: " + e.getMessage(), e);
        }
        LOG.info("TailDir source {} stopped.", getName());
    }

    @Override
    public String toString() {
        return String.format("TailDir source: { positionFile: %s, skipToEnd: %s, " +
                        "byteOffsetHeader: %s, idleTimeout: %s, writePosInterval: %s }", positionFilePath, skipToEnd,
                byteOffsetHeader, idleTimeout, writePosInterval);
    }

    @Override
    public long getBatchSize() {
        return batchSize;
    }

    @Override
    public void configure(Context context) {
        String fileGroups = context.getString(FILE_GROUPS);
        Preconditions.checkState(fileGroups != null, "Missing param: " + FILE_GROUPS);

        filePaths = selectByKeys(context.getSubProperties(FILE_GROUPS_PREFIX), fileGroups.split("\\s+"));
        Preconditions.checkState(!filePaths.isEmpty(),
                "Mapping for tailing files is empty or invalid: '" + FILE_GROUPS_PREFIX + "'");

        String homePath = System.getProperty("user.home").replace('\\', '/');
        positionFilePath = context.getString(POSITION_FILE, homePath + DEFAULT_POSITION_FILE);
        Path positionFile = Paths.get(positionFilePath);
        try {
            Files.createDirectories(positionFile.getParent());
        } catch (IOException e) {
            throw new FlumeException("Error creating positionFile parent directories", e);
        }
        headerTable = getTable(context, HEADERS_PREFIX);
        batchSize = context.getInteger(BATCH_SIZE, DEFAULT_BATCH_SIZE);
        skipToEnd = context.getBoolean(SKIP_TO_END, DEFAULT_SKIP_TO_END);
        byteOffsetHeader = context.getBoolean(BYTE_OFFSET_HEADER, DEFAULT_BYTE_OFFSET_HEADER);
        idleTimeout = context.getInteger(IDLE_TIMEOUT, DEFAULT_IDLE_TIMEOUT);
        writePosInterval = context.getInteger(WRITE_POS_INTERVAL, DEFAULT_WRITE_POS_INTERVAL);
        cachePatternMatching = context.getBoolean(CACHE_PATTERN_MATCHING, DEFAULT_CACHE_PATTERN_MATCHING);

        backoffSleepIncrement = context.getLong(PollableSourceConstants.BACKOFF_SLEEP_INCREMENT,
                PollableSourceConstants.DEFAULT_BACKOFF_SLEEP_INCREMENT);
        maxBackOffSleepInterval = context.getLong(PollableSourceConstants.MAX_BACKOFF_SLEEP,
                PollableSourceConstants.DEFAULT_MAX_BACKOFF_SLEEP);
        fileHeader = context.getBoolean(FILENAME_HEADER, DEFAULT_FILE_HEADER);
        fileHeaderKey = context.getString(FILENAME_HEADER_KEY, DEFAULT_FILENAME_HEADER_KEY);
        maxBatchCount = context.getLong(MAX_BATCH_COUNT, DEFAULT_MAX_BATCH_COUNT);
        if (maxBatchCount <= 0) {
            maxBatchCount = DEFAULT_MAX_BATCH_COUNT;
            LOG.warn("Invalid maxBatchCount specified, initializing source " + "default maxBatchCount of {}",
                    maxBatchCount);
        }
        Map<String, String> interceptorParams = context.getSubProperties("interceptors.");
        Context interceptorContext = new Context(interceptorParams);
        super.configure(interceptorContext);
    }

    @Override
    public Status process() throws EventDeliveryException {
        Status status = BACKOFF;
        try {
            existingInodes.clear();
            existingInodes.addAll(reader.updateTailFiles());
            for (long inode : existingInodes) {
                TailFile tf = reader.getTailFiles().get(inode);
                if (tf.needTail()) {
                    boolean hasMoreLines = tailFileProcess(tf, true);
                    if (hasMoreLines) {
                        status = Status.READY;
                    }
                }
            }
            closeTailFiles();
        } catch (Throwable t) {
            LOG.error("Unable to tail files", t);
            status = BACKOFF;
        }
        return status;
    }

    @Override
    public long getBackOffSleepIncrement() {
        return backoffSleepIncrement;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return maxBackOffSleepInterval;
    }

    private Map<String, String> selectByKeys(Map<String, String> map, String[] keys) {
        Map<String, String> result = Maps.newHashMap();
        for (String key : keys) {
            if (map.containsKey(key)) {
                result.put(key, map.get(key));
            }
        }
        return result;
    }

    private Table<String, String, String> getTable(Context context, String headersPrefix) {
        Table<String, String, String> table = HashBasedTable.create();
        for (Map.Entry<String, String> e : context.getSubProperties(headersPrefix).entrySet()) {
            String[] parts = e.getKey().split("\\.", 2);
            table.put(parts[0], parts[1], e.getValue());
        }
        return table;
    }

    private void closeTailFiles() throws IOException, InterruptedException {
        for (long inode : idleInodes) {
            TailFile tf = reader.getTailFiles().get(inode);
            if (tf.getRaf() != null) { // when file has not closed yet
                tailFileProcess(tf, false);
                tf.close();
                LOG.info("Closed file: " + tf.getPath() + ", inode: " + inode + ", pos: " + tf.getPos());
            }
        }
        idleInodes.clear();
    }

    private boolean tailFileProcess(TailFile tf, boolean backoffWithoutNL) throws IOException, InterruptedException {
        long batchCount = 0;
        while (true) {
            reader.setCurrentFile(tf);
            List<Event> events = reader.readEvents(batchSize, backoffWithoutNL);
            reader.commit();
            if (events.isEmpty()) {
                return false;
            }
            while (events.size() < batchSize) {
                List<Event> nextEvent = reader.readEvents(batchSize - events.size(), backoffWithoutNL);
                reader.commit();
                events.addAll(nextEvent);
                if (nextEvent.size() < batchSize / 3) {
                    break;
                }
            }

            try {
                events = getInterceptorChain().intercept(events);
                getSinkRunner().batchProcess(events);
            } catch (Exception ex) {
                LOG.warn("The source send event failed, ", ex);
                while (true) {
                    TimeUnit.MILLISECONDS.sleep(retryInterval);
                    retryInterval = retryInterval << 1;
                    retryInterval = Math.min(retryInterval, maxRetryInterval);
                    try {
                        if (getSinkRunner().batchProcess(events)) {
                            break;
                        }
                    } catch (Exception ex1) {
                        LOG.warn("The source send event failed, ", ex);
                    }
                }
                continue;
            }
            retryInterval = 1000;
            if (events.size() < batchSize) {
                return false;
            }
            if (++batchCount >= maxBatchCount) {
                return true;
            }
        }
    }

    /**
     * Runnable class that checks whether there are files which should be closed.
     */
    private class idleFileCheckerRunnable implements Runnable {

        @Override
        public void run() {
            try {
                long now = System.currentTimeMillis();
                for (TailFile tf : reader.getTailFiles().values()) {
                    if (tf.getLastUpdated() + idleTimeout < now && tf.getRaf() != null) {
                        idleInodes.add(tf.getInode());
                    }
                }
            } catch (Throwable t) {
                LOG.error("Uncaught exception in IdleFileChecker thread", t);
            }
        }
    }

    /**
     * Runnable class that writes a position file which has the last read position
     * of each file.
     */
    private class PositionWriterRunnable implements Runnable {

        @Override
        public void run() {
            writePosition();
        }
    }

    private void writePosition() {
        File file = new File(positionFilePath);
        FileWriter writer = null;
        try {
            writer = new FileWriter(file);
            if (!existingInodes.isEmpty()) {
                String json = toPosInfoJson();
                writer.write(json);
            }
        } catch (Throwable t) {
            LOG.error("Failed writing positionFile", t);
        } finally {
            try {
                if (writer != null) {
                    writer.close();
                }
            } catch (IOException e) {
                LOG.error("Error: " + e.getMessage(), e);
            }
        }
    }

    private String toPosInfoJson() {
        @SuppressWarnings("rawtypes")
        List<Map> posInfos = Lists.newArrayList();
        for (Long inode : existingInodes) {
            TailFile tf = reader.getTailFiles().get(inode);
            posInfos.add(ImmutableMap.of("inode", inode, "pos", tf.getPos(), "file", tf.getPath()));
        }
        return new Gson().toJson(posInfos);
    }
}