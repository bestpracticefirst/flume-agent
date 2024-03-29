
package com.zhw.flume.sink.kafka;

import com.google.common.base.Throwables;
import com.zhw.flume.conf.Configurable;
import com.zhw.flume.sink.AbstractSink;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.BatchSizeSupported;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.formatter.output.BucketPath;
import org.apache.flume.instrumentation.kafka.KafkaSinkCounter;
import org.apache.flume.shared.kafka.KafkaSSLUtil;
import org.apache.flume.sink.kafka.KafkaSinkConstants;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.utils.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Future;

import static org.apache.flume.sink.kafka.KafkaSinkConstants.BATCH_SIZE;
import static org.apache.flume.sink.kafka.KafkaSinkConstants.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.flume.sink.kafka.KafkaSinkConstants.BROKER_LIST_FLUME_KEY;
import static org.apache.flume.sink.kafka.KafkaSinkConstants.DEFAULT_ACKS;
import static org.apache.flume.sink.kafka.KafkaSinkConstants.DEFAULT_BATCH_SIZE;
import static org.apache.flume.sink.kafka.KafkaSinkConstants.DEFAULT_KEY_SERIALIZER;
import static org.apache.flume.sink.kafka.KafkaSinkConstants.DEFAULT_TOPIC;
import static org.apache.flume.sink.kafka.KafkaSinkConstants.DEFAULT_VALUE_SERIAIZER;
import static org.apache.flume.sink.kafka.KafkaSinkConstants.KAFKA_PRODUCER_PREFIX;
import static org.apache.flume.sink.kafka.KafkaSinkConstants.KEY_HEADER;
import static org.apache.flume.sink.kafka.KafkaSinkConstants.KEY_SERIALIZER_KEY;
import static org.apache.flume.sink.kafka.KafkaSinkConstants.MESSAGE_SERIALIZER_KEY;
import static org.apache.flume.sink.kafka.KafkaSinkConstants.OLD_BATCH_SIZE;
import static org.apache.flume.sink.kafka.KafkaSinkConstants.REQUIRED_ACKS_FLUME_KEY;
import static org.apache.flume.sink.kafka.KafkaSinkConstants.TOPIC_CONFIG;


/**
 * A Flume Sink that can publish messages to Kafka.
 * This is a general implementation that can be used with any Flume agent and
 * a channel.
 * The message can be any event and the key is a string that we read from the
 * header
 * For use of partitioning, use an interceptor to generate a header with the
 * partition key
 * <p/>
 * Mandatory properties are:
 * brokerList -- can be a partial list, but at least 2 are recommended for HA
 * <p/>
 * <p/>
 * however, any property starting with "kafka." will be passed along to the
 * Kafka producer
 * Read the Kafka producer documentation to see which configurations can be used
 * <p/>
 * Optional properties
 * topic - there's a default, and also - this can be in the event header if
 * you need to support events with
 * different topics
 * batchSize - how many messages to process in one batch. Larger batches
 * improve throughput while adding latency.
 * requiredAcks -- 0 (unsafe), 1 (accepted by at least one broker, default),
 * -1 (accepted by all brokers)
 * useFlumeEventFormat - preserves event headers when serializing onto Kafka
 * <p/>
 * header properties (per event):
 * topic
 * key
 */

/**
 * 因为所有服务共享改动，所以单切出来一个用于测试改动
 *
 * @author zhw
 */
public class KafkaSink extends AbstractSink implements Configurable, BatchSizeSupported {

    private static final Logger LOG = LoggerFactory.getLogger(org.apache.flume.sink.kafka.KafkaSink.class);

    private final Properties kafkaProps = new Properties();

    private KafkaProducer<String, byte[]> producer;

    private String topic;

    private int batchSize;

    private List<Future<RecordMetadata>> kafkaFutures;

    private boolean useAvroEventFormat;

    private String partitionHeader = null;

    private Integer staticPartitionId = null;

    private boolean allowTopicOverride;

    private String topicHeader = null;

    private Optional<SpecificDatumWriter<AvroFlumeEvent>> writer = Optional.empty();

    private Optional<ByteArrayOutputStream> tempOutStream = Optional.empty();

    //Fine to use null for initial value, Avro will create new ones if this
    // is null
    private BinaryEncoder encoder = null;


    //For testing
    public String getTopic() {
        return topic;
    }

    @Override
    public long getBatchSize() {
        return batchSize;
    }


    @Override
    public synchronized void start() {
        // instantiate the producer
        producer = new KafkaProducer<String, byte[]>(kafkaProps);
        super.start();
    }

    @Override
    public synchronized void stop() {
        producer.close();
        super.stop();
    }


    /**
     * We configure the sink and generate properties for the Kafka Producer
     * <p>
     * Kafka producer properties is generated as follows:
     * 1. We generate a properties object with some static defaults that
     * can be overridden by Sink configuration
     * 2. We add the configuration users added for Kafka (parameters starting
     * with .kafka. and must be valid Kafka Producer properties
     * 3. We add the sink's documented parameters which can override other
     * properties
     *
     * @param context
     */
    @Override
    public void configure(Context context) {

        translateOldProps(context);

        String topicStr = context.getString(TOPIC_CONFIG);
        if (topicStr == null || topicStr.isEmpty()) {
            topicStr = DEFAULT_TOPIC;
            LOG.warn("Topic was not specified. Using {} as the topic.", topicStr);
        } else {
            LOG.info("Using the static topic {}. This may be overridden by event headers", topicStr);
        }

        topic = topicStr;

        batchSize = context.getInteger(BATCH_SIZE, DEFAULT_BATCH_SIZE);

        useAvroEventFormat = context.getBoolean(KafkaSinkConstants.AVRO_EVENT, KafkaSinkConstants.DEFAULT_AVRO_EVENT);

        partitionHeader = context.getString(KafkaSinkConstants.PARTITION_HEADER_NAME);
        staticPartitionId = context.getInteger(KafkaSinkConstants.STATIC_PARTITION_CONF);

        allowTopicOverride = context.getBoolean(KafkaSinkConstants.ALLOW_TOPIC_OVERRIDE_HEADER,
                KafkaSinkConstants.DEFAULT_ALLOW_TOPIC_OVERRIDE_HEADER);

        topicHeader = context.getString(KafkaSinkConstants.TOPIC_OVERRIDE_HEADER,
                KafkaSinkConstants.DEFAULT_TOPIC_OVERRIDE_HEADER);

        kafkaFutures = new LinkedList<Future<RecordMetadata>>();

        String bootStrapServers = context.getString(BOOTSTRAP_SERVERS_CONFIG);
        if (bootStrapServers == null || bootStrapServers.isEmpty()) {
            throw new ConfigurationException("Bootstrap Servers must be specified");
        }

        setProducerProps(context, bootStrapServers);
    }

    @Override
    public boolean process(List<Event> events) throws EventDeliveryException {
        kafkaFutures.clear();
        String eventTopic = null;
        String eventKey = null;
        if (events == null || events.size() == 0) {
            return true;
        }

        for (Event event : events) {
            Map<String, String> headers = event.getHeaders();
            if (allowTopicOverride) {
                eventTopic = headers.get(topicHeader);
                if (eventTopic == null) {
                    eventTopic = BucketPath.escapeString(topic, event.getHeaders());
                    LOG.debug("{} was set to true but header {} was null. Producing to {}" + " topic instead.",
                            KafkaSinkConstants.ALLOW_TOPIC_OVERRIDE_HEADER, topicHeader, eventTopic);
                }
            } else {
                eventTopic = topic;
            }
            eventKey = headers.get(KEY_HEADER);
            Integer partitionId = null;
            try {
                ProducerRecord<String, byte[]> record;
                if (staticPartitionId != null) {
                    partitionId = staticPartitionId;
                }
                //Allow a specified header to override a static ID
                if (partitionHeader != null) {
                    String headerVal = event.getHeaders().get(partitionHeader);
                    if (headerVal != null) {
                        partitionId = Integer.parseInt(headerVal);
                    }
                }
                if (partitionId != null) {
                    record = new ProducerRecord<String, byte[]>(eventTopic, partitionId, eventKey,
                            serializeEvent(event, useAvroEventFormat));
                } else {
                    record = new ProducerRecord<String, byte[]>(eventTopic, eventKey,
                            serializeEvent(event, useAvroEventFormat));
                }
                kafkaFutures.add(producer.send(record, new SinkCallback()));
            } catch (NumberFormatException ex) {
                throw new EventDeliveryException("Non integer partition id specified", ex);
            } catch (Exception ex) {
                throw new EventDeliveryException("Could not send event", ex);
            }
        }
        producer.flush();
        try {
            for (Future<RecordMetadata> future : kafkaFutures) {
                future.get();
            }
        } catch (Exception ex) {
            String errorMsg = "Failed to publish events";
            LOG.error("Failed to publish events", ex);
            throw new EventDeliveryException(errorMsg, ex);
        }
        return true;
    }

    private void translateOldProps(Context ctx) {

        if (!(ctx.containsKey(TOPIC_CONFIG))) {
            ctx.put(TOPIC_CONFIG, ctx.getString("topic"));
            LOG.warn("{} is deprecated. Please use the parameter {}", "topic", TOPIC_CONFIG);
        }

        //Broker List
        // If there is no value we need to check and set the old param and log a warning message
        if (!(ctx.containsKey(BOOTSTRAP_SERVERS_CONFIG))) {
            String brokerList = ctx.getString(BROKER_LIST_FLUME_KEY);
            if (brokerList == null || brokerList.isEmpty()) {
                throw new ConfigurationException("Bootstrap Servers must be specified");
            } else {
                ctx.put(BOOTSTRAP_SERVERS_CONFIG, brokerList);
                LOG.warn("{} is deprecated. Please use the parameter {}", BROKER_LIST_FLUME_KEY,
                        BOOTSTRAP_SERVERS_CONFIG);
            }
        }

        //batch Size
        if (!(ctx.containsKey(BATCH_SIZE))) {
            String oldBatchSize = ctx.getString(OLD_BATCH_SIZE);
            if (oldBatchSize != null && !oldBatchSize.isEmpty()) {
                ctx.put(BATCH_SIZE, oldBatchSize);
                LOG.warn("{} is deprecated. Please use the parameter {}", OLD_BATCH_SIZE, BATCH_SIZE);
            }
        }

        // Acks
        if (!(ctx.containsKey(KAFKA_PRODUCER_PREFIX + ProducerConfig.ACKS_CONFIG))) {
            String requiredKey = ctx.getString(KafkaSinkConstants.REQUIRED_ACKS_FLUME_KEY);
            if (!(requiredKey == null) && !(requiredKey.isEmpty())) {
                ctx.put(KAFKA_PRODUCER_PREFIX + ProducerConfig.ACKS_CONFIG, requiredKey);
                LOG.warn("{} is deprecated. Please use the parameter {}", REQUIRED_ACKS_FLUME_KEY,
                        KAFKA_PRODUCER_PREFIX + ProducerConfig.ACKS_CONFIG);
            }
        }

        if (ctx.containsKey(KEY_SERIALIZER_KEY)) {
            LOG.warn("{} is deprecated. Flume now uses the latest Kafka producer which implements " +
                            "a different interface for serializers. Please use the parameter {}", KEY_SERIALIZER_KEY,
                    KAFKA_PRODUCER_PREFIX + ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
        }

        if (ctx.containsKey(MESSAGE_SERIALIZER_KEY)) {
            LOG.warn("{} is deprecated. Flume now uses the latest Kafka producer which implements " +
                            "a different interface for serializers. Please use the parameter {}", MESSAGE_SERIALIZER_KEY,
                    KAFKA_PRODUCER_PREFIX + ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
        }
    }

    private void setProducerProps(Context context, String bootStrapServers) {
        kafkaProps.clear();
        kafkaProps.put(ProducerConfig.ACKS_CONFIG, DEFAULT_ACKS);
        //Defaults overridden based on config
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, DEFAULT_KEY_SERIALIZER);
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, DEFAULT_VALUE_SERIAIZER);
        kafkaProps.putAll(context.getSubProperties(KAFKA_PRODUCER_PREFIX));
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);

        KafkaSSLUtil.addGlobalSSLParameters(kafkaProps);
    }

    protected Properties getKafkaProps() {
        return kafkaProps;
    }

    private byte[] serializeEvent(Event event, boolean useAvroEventFormat) throws IOException {
        byte[] bytes;
        if (useAvroEventFormat) {
            if (!tempOutStream.isPresent()) {
                tempOutStream = Optional.of(new ByteArrayOutputStream());
            }
            if (!writer.isPresent()) {
                writer = Optional.of(new SpecificDatumWriter<AvroFlumeEvent>(AvroFlumeEvent.class));
            }
            tempOutStream.get().reset();
            AvroFlumeEvent e = new AvroFlumeEvent(toCharSeqMap(event.getHeaders()), ByteBuffer.wrap(event.getBody()));
            encoder = EncoderFactory.get().directBinaryEncoder(tempOutStream.get(), encoder);
            writer.get().write(e, encoder);
            encoder.flush();
            bytes = tempOutStream.get().toByteArray();
        } else {
            bytes = event.getBody();
        }
        return bytes;
    }

    private static Map<CharSequence, CharSequence> toCharSeqMap(Map<String, String> stringMap) {
        Map<CharSequence, CharSequence> charSeqMap = new HashMap<CharSequence, CharSequence>();
        for (Map.Entry<String, String> entry : stringMap.entrySet()) {
            charSeqMap.put(entry.getKey(), entry.getValue());
        }
        return charSeqMap;
    }

}

