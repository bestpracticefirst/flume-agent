
package com.zhw.flume.sink.kafka;


import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zhw
 */
class SinkCallback implements Callback {
    private static final Logger LOG = LoggerFactory.getLogger(SinkCallback.class);

    public SinkCallback() {
    }

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (exception != null) {
            LOG.error("Error sending message to Kafka {} ", exception.getMessage());
        }
    }
}